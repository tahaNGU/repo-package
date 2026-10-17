# Implementation plan

## Scope

A transactional outbox library for Go. Two halves, deliberately decoupled:

1. **Enqueue (synchronous, in the caller's transaction).** The application inserts outbox
   rows using *its own* `*sql.Tx`, so business rows and outbox rows share one commit.
2. **Publish (asynchronous, in a background worker).** A worker leases rows, hands them to
   a `Publisher`, and records the outcome. No broker call ever happens inside a DB
   transaction.

## Packages

| Package | Purpose | Dependencies |
| --- | --- | --- |
| `outbox` | domain model, `Store`/`Publisher`/`Observer`/`Tracer` interfaces, config, backoff, worker | stdlib only |
| `outbox/postgres` | reference `Store`, SQL, embedded migrations | stdlib only (`database/sql`) |
| `outbox/memstore` | in-memory `Store` + fake tx + fault injection, for user and library tests | stdlib only |
| `outbox/contrib/prometheus` | `Observer` implementation (separate module) | prometheus/client_golang |
| `outbox/contrib/otel` | `Tracer` implementation + W3C propagation (separate module) | go.opentelemetry.io/otel |
| `outbox/postgres/pgxcompat` | compatibility test proving driver independence (separate module) | jackc/pgx/v5 (test only) |
| `outbox/examples/basic` | runnable end-to-end example (separate module) | lib/pq |

The core module has **zero** non-stdlib build dependencies; `lib/pq` is a test-only
dependency used by the Postgres integration tests and benchmarks. Everything else
lives in its own module, each with a `replace` pointing at the working tree, so
core consumers never link Prometheus, OpenTelemetry or a database driver.

## State machine

```
                    enqueue
                       |
                       v
                   [pending] <-------------------+
                       |                         |
                 claim | attempts+1              | Reschedule(delay)   retryable &
                       | lease_epoch+1           | Release()           attempts<max
                       v                         |                     or shutdown
                   [leased] ---------------------+
                     |   |
         publish ok  |   | permanent error, or attempts >= MaxAttempts
                     v   v
            [published]  [failed] --Requeue()--> [pending]
```

* `attempts` is incremented **at claim time**, so a message that crashes its worker
  cannot loop forever.
* Every worker-side transition is fenced by `(id, lease_epoch, state='leased')`. A stale
  worker whose lease was reclaimed gets `ErrLeaseLost` and cannot overwrite newer state.
* An expired lease is reclaimed by the ordinary claim query (no separate recovery
  goroutine), which also increments `lease_epoch` and thereby fences the old owner.

## Concurrency model

One poller goroutine plus `Concurrency` publisher goroutines, fed over an unbuffered
channel so that a hand-off only completes when a publisher actually takes the message.
The poller claims only as many rows as there are *currently idle* publishers (bounded by
`BatchSize`), so:

* leases are held only for `PublishTimeout + StoreTimeout`, which config validation
  forces to be `<= LeaseDuration`;
* there is no in-memory queue of leased work to lose on crash;
* backpressure is natural: a slow broker simply stops the poller from claiming.

## Decisions

| Decision | Alternative | Why |
| --- | --- | --- |
| `FOR UPDATE SKIP LOCKED` claim in a CTE + fencing `lease_epoch` | advisory locks; `UPDATE .. RETURNING` without CTE | standard, scales to many workers, single round trip, gives ordering control |
| DB clock (`now()`) for all state timestamps | worker clock | no clock skew between workers; leases cannot be mis-expired |
| Poll (with an in-process `Notify` hint) | `LISTEN/NOTIFY`, logical decoding | simplest reliable mechanism; polling is the fallback those designs need anyway |
| Claim <= idle publishers | claim a large batch into memory | shortest lease hold, no in-memory backlog, simpler validation |
| Per-message ack | batched acks | precise per-message error handling; acks are not the measured bottleneck |
| UUIDv7 ids generated in-process | DB-generated ids | ids are stable across retries and known before commit; time-ordered for index locality |
| `attempts++` at claim | `attempts++` on failure | bounds poison messages that kill the worker |
| Opt-in per-`ordering_key` serialization | always-on ordering; no ordering | one extra `NOT EXISTS` clause and one partial index, only when asked for |
| Interfaces for metrics/tracing, adapters in separate modules | direct prometheus/otel dependency | core stays dependency-free, integration stays trivial |

## Acceptance criteria

1. `go build ./...`, `go vet ./...` clean; all exported identifiers documented.
2. Business write + `Enqueue` in one `*sql.Tx`; rollback leaves no outbox row (tested).
3. Concurrent workers never double-claim a row under healthy leases (tested with N
   workers against real Postgres).
4. Stale worker updates are rejected after reclaim (`ErrLeaseLost`, tested).
5. Retry schedule is persisted (`available_at`, `attempts`), so a restart resumes it
   (tested by restarting a worker).
6. Retry budget exhaustion yields a terminal `failed` row that is listable and requeueable.
7. `Shutdown` is bounded, idempotent, concurrency-safe, leaks no goroutines, and never
   marks an unpublished message as published (tested).
8. Metrics via `Observer`, structured logs via `log/slog`, tracing via `Tracer`.
9. Unit tests for backoff, validation, classification, ids, state transitions; race
   detector clean; failure-injection tests for every crash window in section 2.7 of the
   brief; benchmarks for enqueue/claim/end-to-end/batch/worker counts.
10. README documents delivery guarantees, duplicates, ordering limits, operations.

## Outcome

Every acceptance criterion above is met and exercised by a test. Two defects were
found during implementation and fixed:

1. The retention sweep used `DELETE ... WHERE id IN (SELECT ... LIMIT n FOR UPDATE
   SKIP LOCKED)`. PostgreSQL re-executed the sub-select per candidate row, so the
   batch limit was ignored and the statement deleted every eligible row. Both
   locking CTEs are now `AS MATERIALIZED` (ADR 3).
2. The claim predicate had two OR-ed branches, which forced a sort of every due row
   before the `LIMIT`. Unifying the meaning of `available_at` reduced it to a single
   index-ordered scan (ADR 4).

One optimisation was made after measuring: building a per-message `slog` logger cost
13 of 25 allocations per message for records that are almost never emitted. Log
attributes are now built only where a record is written (25 -> 11 allocations).

A panicking publisher used to take the process down and leak a worker slot; it is now
contained and recorded as a permanent failure.
