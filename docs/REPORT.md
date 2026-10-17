# Engineering report

A transactional outbox library for Go, implemented from an empty repository.
6,000 lines of Go across six modules: 3,700 of library code, 4,800 of tests,
100 of SQL, and the documentation.

## 1. Architecture

Two halves that share nothing but the table.

**Enqueue** is synchronous and transactional. `postgres.Store.Enqueue` takes the
application's own `*sql.Tx` and does one INSERT on it, so the business rows and
the outbox rows reach the same commit. It contacts no broker, which is why a
broker outage cannot fail a request.

**Publish** is asynchronous. A `Worker` runs one poller goroutine and
`Concurrency` publisher goroutines. The poller claims only as many rows as it
has *idle* publishers and hands them over an unbuffered channel, so nothing
leased is ever parked in memory, at most `Concurrency` messages are leased at
any instant, and a slow broker simply stops the worker from claiming. Two more
goroutines sample queue depth and, if configured, delete published rows.

```
enqueue ──▶ [pending] ◀─────────────┐
                │                   │ Reschedule(delay) / Release()
         claim  │ attempts+1        │
                │ lease_epoch+1     │
                ▼                   │
            [leased] ───────────────┘
              │   │
   publish ok │   │ permanent error, or attempts >= MaxAttempts,
              ▼   ▼ or an undecodable row
      [published] [failed] ──Requeue()──▶ [pending]
```

Claiming is one statement: select due rows `FOR UPDATE SKIP LOCKED`, take
ownership, and increment `attempts` and `lease_epoch`. Every later write is
conditional on `(id, lease_epoch, state='leased')`, so a worker that lost its
lease cannot overwrite the state written by the worker that took over. Expired
leases are reclaimed by the ordinary claim query — there is no separate
recovery pass.

Packages: `outbox` (model, interfaces, worker — standard library only),
`outbox/postgres` (store, SQL, migrations), `outbox/memstore` (in-memory store
for tests), and separate modules for Prometheus, OpenTelemetry, the example and
a pgx compatibility test.

## 2. Key design decisions

Full reasoning in [ADR.md](ADR.md); the ones that shaped the code most:

* **A `lease_epoch` fencing token, incremented on every claim.** Stale writes
  are rejected structurally rather than by being careful. Cheap, readable in
  logs, needs no extension.
* **`AS MATERIALIZED` on both locking CTEs.** Not stylistic — see §9.
* **One meaning for `available_at`: "when a worker may take this row."** The
  next attempt time for a pending row, the lease expiry for a leased one. This
  collapses the claim predicate to a single branch, which lets one partial
  index serve it in order and stop at the `LIMIT` instead of sorting every due
  row.
* **The database clock for every stored timestamp.** Store methods take
  durations, never absolute times, so clock skew between workers is not part of
  the correctness argument.
* **`attempts` counted at claim time, not on failure.** A payload that kills
  its worker spends its budget and becomes `failed` instead of looping forever.
* **Claim at most as many rows as there are idle publishers.** Makes
  `PublishTimeout + StoreTimeout <= LeaseDuration` a checkable invariant.
* **Polling with an in-process `Notify` hint.** Correctness never depends on
  the hint; `LISTEN/NOTIFY` would need polling as a fallback anyway.
* **Observability behind interfaces, adapters in separate modules.** The core
  module has no non-stdlib build dependencies, and metric cardinality is a
  property of the API rather than a convention.

## 3. Public API

```go
// Model
type ID [16]byte                       // UUIDv7; stable across retries
func NewID() ID; func ParseID(string) (ID, error)
type Message struct { ID; Type, ContentType string; Payload []byte
                      AggregateType, AggregateID, OrderingKey string
                      Headers map[string]string; AvailableAt, CreatedAt time.Time }

// Enqueue (adapter-typed, takes the caller's transaction)
func (*postgres.Store) Enqueue(ctx, tx outbox.Execer, msgs ...Message) error

// Publish
type Publisher interface { Publish(ctx, Message) error }
func Permanent(err error) error         // non-retryable
func NewWorker(Store, Publisher, Config) (*Worker, error)
func (*Worker) Start(ctx) error
func (*Worker) Shutdown(ctx) error      // bounded, idempotent
func (*Worker) Notify()

// Storage contract (worker side only; 6 methods)
type Store interface {
    Claim(ctx, ClaimRequest) (ClaimResult, error)
    MarkPublished(ctx, ID, leaseEpoch int64) error
    Reschedule(ctx, ID, leaseEpoch int64, delay time.Duration, Failure) error
    Fail(ctx, ID, leaseEpoch int64, Failure) error
    Release(ctx, ID, leaseEpoch int64) error
    Stats(ctx) (Stats, error)
}
type Pruner interface { DeletePublished(ctx, olderThan, limit) (int, error) }
type Inspector interface { ListFailed(...); Requeue(...) }

// Observability (optional, no dependencies)
type Observer interface { ... }         // embed NopObserver
type Tracer interface { StartPublish(ctx, *Claimed) (context.Context, EndSpan) }

// Errors
ErrInvalidMessage, ErrInvalidConfig, ErrLeaseLost, ErrNotFound,
ErrWorkerRunning, ErrWorkerStopped, ErrShutdownTimeout
```

37 exported identifiers in the core package, 11 in `postgres`. Every one is
documented; `go doc` has no gaps.

## 4. Database

One table, three partial indexes, two migrations (the second only for ordered
delivery). Rendered for any table name by `postgres.Migrations`, or applied
idempotently by `Store.Migrate`.

| Index | Serves |
| --- | --- |
| `(available_at, seq) WHERE state IN ('pending','leased')` | claiming, reclaiming expired leases, depth, lag |
| `(seq) WHERE state = 'failed'` | listing and counting terminal failures |
| `(published_at) WHERE state = 'published'` | retention sweeps |

Four `CHECK` constraints keep rows internally consistent (known states,
non-negative attempts, `leased` iff there is a lease owner, `published` iff
there is a publication time) — all four are verified by a test that tries to
violate them.

Measured plans on a 100k-row table with 60 unpublished rows:

```
claim:   Limit → LockRows → Index Scan using <t>_due_idx (actual rows=10)
         Index Cond: (available_at <= now())        -- no Sort node
depth:   Aggregate → Index Scan using <t>_due_idx (actual rows=50)
lag:     Result → InitPlan → Limit → Index Scan   (reads one index entry)
ordered: + Index Only Scan using <t>_ordering_idx, Heap Fetches: 0
```

Retention is opt-in, deletes only `published` rows, and is restricted on that
state in both the row selection and the `DELETE`.

## 5. Delivery and retry guarantees

Guaranteed: a committed message is **eventually published or ends in the failed
state**, never silently dropped; a rolled-back message is **never published**;
under healthy leases **at most one worker owns a message**; a stale worker
**cannot overwrite** the new owner's state; retry schedules are **persisted**.

Not guaranteed: **exactly-once delivery**. If the broker accepts a message and
the worker dies before recording it, the only options are to publish again or
to drop it; this package publishes again. Closing that window needs the broker
and the database to commit together. **Consumers must be idempotent** —
`Message.ID` is stable across every attempt and is the deduplication key. There
is no global ordering; per-key serialisation is opt-in with precisely
documented limits (head-of-line blocking on failure, duplicates can still
reorder, and it assumes the application serialises writes per key).

Retries use exponential backoff with jitter, stored as `available_at`, so a
deployment mid-backoff does not reset it. Terminal failure is reached on a
`Permanent` error, on budget exhaustion, or on an undecodable row — all three
are listable and requeueable, and nothing is deleted automatically.

Every failure window from the brief is enumerated in the README and the package
documentation, and each has a test (§7).

## 6. Observability

* **Metrics** via `Observer`: messages claimed, published by outcome, retries
  scheduled, terminal failures by reason, expired leases, lost leases, store
  errors by operation, publish duration and lag histograms, batch size, active
  publishes, sampled depth and backlog age. Labels are a closed set plus the
  message type; ids, payloads and error strings are never labels, which a test
  asserts by walking the registry.
* **Logs** via `log/slog` with `message_id`, `message_type`, `attempt`,
  `batch_size`, `operation`, `error_kind` and durations. Payloads are never
  logged. Attributes are built only where a record is emitted, so quiet
  operation allocates nothing for logging.
* **Tracing** via `Tracer`, with the trace context carried in the message
  headers as a W3C `traceparent`, so a publish minutes later still links to the
  transaction that produced it. Verified by a test that asserts the publish
  span's parent is the enqueueing span, marked remote.
* **Cost disclosure**: counters are in-process and cheap; the depth sample runs
  three index scans plus one index lookup on `StatsInterval`, never per
  message; nothing on the publishing path scans the table.

## 7. Tests

**120 tests pass; `-race` is clean; five consecutive full race runs showed no
flakiness.** `go vet` is clean across all six modules, and `gofmt` reports
nothing.

| Group | Covers |
| --- | --- |
| Unit | UUIDv7 generation/parse/scan, backoff schedule, jitter bounds, overflow, config defaults and every rejection, message validation, error classification, truncation, traceparent round trip |
| Transaction | Business write + enqueue in one `*sql.Tx` committing together; rollback leaving neither; a batch split across statements rolling back atomically; invalid messages rejected before touching the database |
| Concurrency | 8 goroutines racing to claim 400 rows with no overlap; 6 independent workers draining 500 messages with exactly 500 deliveries; concurrency ceiling respected; concurrent `Shutdown` from 8 goroutines; goroutine-leak check |
| Fencing | Expired lease reclaimed with an advanced epoch; all four stale updates rejected with `ErrLeaseLost`; the new owner's state survives; `ErrNotFound` for unknown ids |
| Failure injection | Broker accepted but the ack failed (duplicate, not loss); crash after claiming; lease expiry mid-publish; database outage and recovery; retry exhaustion; undecodable row; publisher panic; publish timeout |
| Shutdown | Waits for in-flight work; bounded when the publisher blocks; releases abandoned messages; never records an unpublished message as published; idempotent; no-op before `Start`; stops on lifetime-context cancellation |
| Ordering | Per-key sequence preserved under 4 workers × 4 goroutines over 200 messages; different keys publish in parallel; a failed message blocks only its key and the key drains in order after requeue |
| Schema | Migration idempotent, indexes present, all four `CHECK` constraints reject bad rows, duplicate ids rejected, claim plan uses the index and does not sort |
| Operational | Stats depth and lag against real rows; retention deletes only published rows and respects its limit; `ListFailed` pagination; `Requeue` touching only failed rows; a store without `Pruner` warning loudly |
| Compatibility | The whole state machine through `pgx/stdlib` instead of `lib/pq`, in its own module |

Integration tests ran against **PostgreSQL 15.15** in Docker. They create and
drop their own table per test and skip entirely without
`OUTBOX_TEST_POSTGRES_DSN`.

## 8. Benchmarks

Environment: Linux 6.8.0 x86_64, 12th Gen Intel Core i7-1260P (16 logical
CPUs), Go 1.24.0, PostgreSQL 15.15 in Docker over loopback TCP with **default
settings** (`fsync` and `synchronous_commit` on). Single machine, so the
database and the benchmark share CPU.

Library overhead — no database, no broker (`-benchtime=20000x`):

| Benchmark | ns/op | msg/s | B/op | allocs/op |
| --- | --- | --- | --- | --- |
| WorkerPipeline workers=1 batch=16 | 3,898 | 256,515 | 1,136 | 15 |
| WorkerPipeline workers=4 batch=32 | 3,932 | 254,350 | 1,081 | 14 |
| WorkerPipeline workers=16 batch=64 | 3,546 | 282,018 | 914 | 11 |
| PublishFailurePath (classify, backoff, summary) | 4,142 | 241,402 | 1,587 | 25 |

Micro (`-benchtime=2s`): `NewID` 378 ns, 0 allocs · `ID.String` 41 ns, 1 alloc ·
`Backoff.Delay` 23 ns, 0 allocs · JSON payload encode 545 ns, 2 allocs.

PostgreSQL operations (`-benchtime=200x`):

| Benchmark | per op | msg/s | allocs/op |
| --- | --- | --- | --- |
| Enqueue batch=1 (incl. commit) | 1.10 ms | 911 | 46 |
| Enqueue batch=10 | 1.35 ms | 7,432 | 263 |
| Enqueue batch=100 | 3.23 ms | 30,949 | 3,150 |
| EnqueueStatements batch=100 (no I/O) | 112 µs | — | 2,217 |
| Claim limit=1 | 953 µs | 1,049 | 84 |
| Claim limit=16 | 1.17 ms | 13,649 | 759 |
| Claim limit=64 | 1.85 ms | 34,639 | 2,919 |
| Claim limit=256 | 4.73 ms | 54,119 | 11,562 |
| Acknowledge (one fenced UPDATE) | 644 µs | — | 13 |
| Stats (50k published + 5k pending) | 2.20 ms | — | 24 |

End-to-end through a running worker, no-op publisher (`-benchtime=3000x`):

| Workers / batch | per message | msg/s |
| --- | --- | --- |
| 1 / 16 | 1.63 ms | 615 |
| 4 / 16 | 823 µs | 1,215 |
| 8 / 32 | 395 µs | 2,530 |
| 16 / 64 | 234 µs | 4,272 |
| 32 / 64 | 189 µs | 5,296 |

**Reading these numbers.** The library costs ~4 µs per message; a commit on this
machine costs 0.6–1.1 ms. Throughput is therefore set almost entirely by
commit latency and how much of it can be overlapped: scaling concurrency from 1
to 32 gives 8.6× throughput, and batching the claim drops its per-message cost
from 953 µs to 18 µs. The remaining per-message cost is the acknowledging
UPDATE, which is why concurrency and replicas are the levers that work. Numbers
on dedicated hardware with a tuned database will differ; measure your own.

One optimisation was made from these measurements: building a per-message
`slog` logger accounted for 13 of 25 allocations per message for records that
are almost never emitted. Moving attribute construction to the point of logging
cut allocations to 11 and improved pipeline throughput ~13%.

## 9. Defects found and fixed during development

1. **The retention sweep ignored its batch limit.** `DELETE ... WHERE id IN
   (SELECT ... ORDER BY published_at LIMIT $n FOR UPDATE SKIP LOCKED)` was
   planned as a nested-loop semi-join that **re-executed the sub-select once per
   candidate row** (`EXPLAIN ANALYZE` showed `loops=10`), so a sweep limited to
   4 rows deleted all 10 eligible rows. In production this would mean unbounded
   delete transactions. Both locking CTEs are now `AS MATERIALIZED`, verified by
   a test that asserts the limit is honoured.
2. **The claim query sorted every due row.** The original two-branch
   (`pending OR expired lease`) predicate could not walk one index in order, so
   PostgreSQL read all matching rows and sorted before applying the `LIMIT` —
   fine on a small table, expensive on a large backlog. Unifying the meaning of
   `available_at` reduced it to an index-ordered scan that stops at the limit,
   asserted by a test that fails on a `Seq Scan` or a `Sort`.
3. **A panicking publisher took the process down and leaked a worker slot.**
   Panics are now contained, logged with a stack, and recorded as a permanent
   failure, so one malformed payload cannot kill the deployment.
4. **A slot-accounting bug in the stop-mid-dispatch path** returned one token
   too many, which could have blocked shutdown. Found by reading the code,
   fixed before it could be observed.
5. **`AvailableAt` in the past behaved differently in the two stores** and
   contradicted its own documentation. It is now clamped to the store's current
   time, so a backfill cannot masquerade as a publishing backlog.
6. **`WithMaxEnqueueBatch` accepted values that exceeded PostgreSQL's limit** of
   65,535 bind parameters. It is now bounded by `MaxEnqueueBatch`.

## 10. Known limitations

* **At-least-once delivery only.** The crash window between broker acceptance
  and the acknowledgement is unavoidable without end-to-end coordination.
  Consumers must be idempotent.
* **No global ordering.** Per-key ordering is opt-in and brings head-of-line
  blocking on failure; an expired lease can still produce a duplicate that
  arrives out of order. Keyed claims also walk past blocked rows, so a long
  blocked backlog makes claiming more expensive.
* **One commit per delivery.** Acknowledgements are not batched, by choice
  (precise error attribution, smaller duplicate window). The ceiling is the
  database's commit latency × concurrency.
* **Depth sampling costs index scans** proportional to the number of rows in
  each state. Exact, but not free: raise `StatsInterval` on a large backlog.
* **Shutdown cannot wait for a publisher that ignores its context.** Such work
  may outlive `Shutdown`; the lease makes it safe, not instant.
* **`Notify` is in-process only.** Cross-process messages wait for the next
  poll.
* **PostgreSQL 12+ only** for the reference store (`AS MATERIALIZED`). No MySQL
  or SQLite adapter; the `Store` interface is small enough to add one, but
  `SKIP LOCKED` semantics differ and would need their own integration tests.
* **No partitioning.** Rejected as unjustified complexity at the scale this
  design targets; retention keeps the table small instead.
* **Attempts are consumed by crashes**, not only by failed publishes. That is
  deliberate poison-message protection, and `Requeue` resets the budget.

## 11. Remaining risks and external dependencies

* **Module path.** `github.com/tahaNGU/gocast` is a placeholder; change it in
  the six `go.mod` files before publishing. The contrib modules require
  `v0.1.0` of the core with a local `replace`, which must exist as a tag at
  release.
* **Build dependencies.** The core module has none beyond the standard library.
  `lib/pq` is test-only; Prometheus, OpenTelemetry and `pgx` are confined to
  their own modules.
* **The publisher is the weak link by design.** The outbox records what
  `Publish` returns, so a publisher that returns `nil` before the broker has
  durably accepted a message will lose it. That contract is documented on the
  interface.
* **Not run here:** no CI pipeline, no load test at sustained production
  volume, no test against PostgreSQL 12/13/14/16/17 (only 15.15), no failover
  or network-partition test against a real cluster, and no test against a real
  broker — every publisher in the suite is a test double.
* **Benchmarks are single-machine** with the database sharing CPU and running
  default settings. Treat them as a relative baseline, not a capacity estimate.
