# outbox

A transactional outbox for Go: change business state and emit an event without
the two ever disagreeing.

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer tx.Rollback()

if err := placeOrder(ctx, tx, order); err != nil {
	return err
}
if err := store.Enqueue(ctx, tx, outbox.Message{
	Type:        "orders.created.v1",
	Payload:     body,
	ContentType: "application/json",
}); err != nil {
	return err
}
return tx.Commit() // the order and its event commit together, or not at all
```

A background worker publishes the stored messages afterwards. No broker call
ever happens inside a database transaction, and the table — not a queue in
memory — is the source of truth.

* Core package depends only on the standard library.
* PostgreSQL reference store using `FOR UPDATE SKIP LOCKED` and lease fencing.
* Concurrent workers across any number of replicas.
* Exponential backoff with jitter, persisted so restarts do not reset it.
* Terminal failure state that can be listed and requeued.
* Metrics, structured logs and tracing behind small interfaces; Prometheus and
  OpenTelemetry adapters live in separate modules.
* Bounded, predictable shutdown.

## Install

```bash
go get github.com/tahaNGU/gocast
```

Requires Go 1.24 and, for the reference store, PostgreSQL 12 or later. Any
driver that registers a `database/sql` driver works — `lib/pq`, or `pgx` through
`pgx/stdlib`.

Optional, each a separate module so you only link what you use:

```bash
go get github.com/tahaNGU/gocast/contrib/prometheus
go get github.com/tahaNGU/gocast/contrib/otel
```

## Run the example

```bash
go run ./examples/basic -dsn 'postgres://user:pass@localhost:5432/db?sslmode=disable'
```

It creates its own tables, writes an order and its event in one transaction,
publishes them with a worker, demonstrates that a rolled-back order leaves no
event behind, and shuts down cleanly.

## Database migration

The schema lives in [`postgres/migrations`](postgres/migrations). Apply it with
your own migration tool by rendering it for your table name:

```go
ms, err := postgres.Migrations(postgres.DefaultTable, false /* ordered delivery */)
for _, m := range ms {
	os.WriteFile(filepath.Join("db/migrations", m.Name), []byte(m.SQL), 0o644)
}
```

Or let the store apply it, which is idempotent and fine for small deployments
and tests:

```go
store, err := postgres.New(db)
err = store.Migrate(ctx)
```

The table carries the message, its delivery state, its attempt count, its
availability time, a lease owner and a monotonic `lease_epoch` fencing token.
Three partial indexes cover the whole workload:

| Index | Covers |
| --- | --- |
| `(available_at, seq) WHERE state IN ('pending','leased')` | claiming, reclaiming expired leases, queue depth, lag |
| `(seq) WHERE state = 'failed'` | listing and counting terminal failures |
| `(published_at) WHERE state = 'published'` | retention sweeps |

They are partial, so published rows — the bulk of a healthy table — are absent
from the claim index entirely and its size tracks the backlog, not the table.

`available_at` means "when a worker may take this row": the next attempt time
for a pending message, and the lease expiry for a leased one. That is what lets
one index and one predicate serve the hot path with no sort.

## Enqueue, then publish

The two halves are deliberately separate:

**Enqueue** is synchronous, transactional and cheap: one INSERT on *your*
`*sql.Tx`. It does not contact a broker, so a broker outage cannot fail your
request. If the transaction rolls back, the event never existed.

**Publishing** is asynchronous. The worker leases a batch, hands each message
to your `Publisher`, and records the outcome. It is the only part that can fail
in interesting ways, and it is the only part that retries.

```go
publisher := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
	// Hand m.Payload to your broker. Use m.ID as the deduplication key.
	// Return outbox.Permanent(err) for failures a retry cannot fix.
	return kafka.Send(ctx, m.Type, m.ID.String(), m.Payload)
})

worker, err := outbox.NewWorker(store, publisher, outbox.Config{})
if err != nil {
	return err
}
if err := worker.Start(ctx); err != nil {
	return err
}
```

## Delivery guarantees

Guaranteed:

* A message committed with its business transaction is **eventually published
  or ends in the failed state**. It is never silently dropped.
* A message whose transaction rolled back is **never published**.
* Under healthy leases, **at most one worker owns a message** at a time.
* A worker that lost its lease **cannot overwrite** the state written by the
  worker that took over (`ErrLeaseLost`).
* Retry schedules are **persisted**, so a restart resumes them rather than
  resetting them.

Not guaranteed:

* **Exactly-once delivery.** Delivery is at-least-once. If the broker accepts
  a message and the worker dies before recording that fact, the only choices
  are to publish again or to drop it; this package publishes again. Closing
  that window would require the broker and the database to commit together.
  **Consumers must be idempotent.** `Message.ID` is stable across every
  attempt — pass it to your broker as a message id or deduplication key and
  discard ids you have already handled.
* **Global ordering.** Messages are claimed oldest-first but published
  concurrently and retried on their own schedules. See
  [ordered delivery](#ordered-delivery-optional) for the per-key option.
* **That your publisher told the truth.** The outbox records what `Publish`
  returns. Return `nil` only once the broker has durably accepted the message.

### Failure windows

| Failure | What happens |
| --- | --- |
| Business transaction rolls back | No outbox row exists; nothing is published |
| Commit succeeds, process dies immediately | The row is committed; another worker publishes it |
| Worker claims, then crashes | The lease expires; another worker retries. One attempt is consumed |
| Broker accepts, worker dies before the ack | The lease expires and the message is published **again**: a duplicate |
| Lease expires mid-publish | Another worker may publish it too; the stale worker's writes are rejected |
| Two workers claim at once | Impossible: claiming is one atomic statement with `SKIP LOCKED` |
| Stale worker writes after reclaim | Rejected with `ErrLeaseLost`; the new owner's state stands |
| Database unavailable | The worker backs off and retries; nothing is lost or acknowledged |
| Broker returns a permanent error | Straight to the failed state, inspectable and requeueable |
| Shutdown during publication | In-flight work is awaited, then cancelled and released for retry |

`attempts` is incremented **when a message is claimed**, not when it fails, so
a payload that kills its worker consumes its budget and ends up failed instead
of looping forever.

## Retries and failure

Retryable failures are rescheduled with exponential backoff and jitter; the
delay is stored as the message's `available_at`, which is why a deployment in
the middle of a backoff does not reset it.

```go
outbox.Config{
	MaxAttempts: 10,
	Backoff: outbox.Backoff{
		Initial:    time.Second,
		Max:        5 * time.Minute,
		Multiplier: 2,
		Jitter:     0.2,
	},
}
```

A message becomes **failed** — terminal, never claimed again — when the
publisher returns `outbox.Permanent(err)`, when it runs out of attempts, or
when its stored row cannot be decoded. Failures are visible and recoverable:

```go
failed, err := store.ListFailed(ctx, 100, outbox.ID{})
n, err := store.Requeue(ctx, ids...) // back to pending with a fresh budget
```

Nothing is ever discarded automatically. Retention only ever deletes rows in
the `published` state, and only if you ask for it:

```go
outbox.Config{Retention: 7 * 24 * time.Hour} // 0 (default) deletes nothing
```

## Configuration

The zero `Config` is valid; every field has a documented default.

| Field | Default | Notes |
| --- | --- | --- |
| `Concurrency` | 8 | Parallel publishes, and the maximum number of simultaneously leased messages |
| `BatchSize` | 64 | Cap on one claim; the effective batch is `min(BatchSize, idle goroutines)` |
| `PollInterval` | 1s | Sleep after finding no work. A full batch polls again immediately |
| `LeaseDuration` | 30s | Claim validity, and the upper bound on crash-recovery delay |
| `PublishTimeout` | 10s | Bounds one `Publish` call |
| `StoreTimeout` | 5s | Bounds one store operation |
| `MaxAttempts` | 10 | Delivery budget per message |
| `Backoff` | 1s → 5m, ×2, 20% jitter | Persisted retry schedule |
| `StatsInterval` | 15s | Queue-depth sampling; negative disables |
| `Retention` | 0 (off) | Deletes published rows older than this |
| `Classify` | `outbox.Classify` | Decides retryable vs permanent |
| `ErrorSummary` | truncate to 512 bytes | Override to redact before storing |
| `Name` | `hostname/pid` | Recorded as the lease owner |
| `Logger` / `Observer` / `Tracer` | `slog.Default()` / no-op / none | |

One relationship is enforced rather than documented:

```
PublishTimeout + StoreTimeout <= LeaseDuration
```

A worker holds a lease for at most one publish plus the write that records its
outcome. A shorter lease would let another worker reclaim a message that is
still being published, which turns into a duplicate. `Config.Validate` rejects
the combination at construction. `BatchSize` is not part of the inequality: the
worker only claims as many messages as it has idle publisher goroutines, so
leased work is never parked in memory waiting for one.

## Observability

### Metrics

Implement `outbox.Observer` (embed `outbox.NopObserver` so future methods do
not break your build), or use the Prometheus module:

```go
reg := prometheus.NewRegistry()
obs, err := outboxprom.New(reg)
worker, err := outbox.NewWorker(store, publisher, outbox.Config{Observer: obs})
```

It exposes messages claimed, published by outcome, retries scheduled, terminal
failures by reason, expired leases, lost leases, store errors by operation,
publish duration and lag histograms, batch size, active publishes, and sampled
queue depth and backlog age.

Labels are a small closed set plus the message type. **Message ids, aggregate
ids, payloads and error strings are never used as labels.** If your types are
not a bounded set, map them with `Options.TypeLabel`.

### Logs

Structured `log/slog`, with `message_id`, `message_type`, `attempt`,
`batch_size`, `operation`, `error_kind` and durations where they apply.
Payloads are never logged. Normal operation logs at debug; retryable failures
at warn; terminal failures, lost leases and store errors at error. Log
attributes are built only when a record is actually emitted, so quiet
operation allocates nothing for logging.

### Tracing

A message is usually published long after the transaction that produced it, so
there is no live parent span. Carry the link in the message instead:

```go
msg.Headers = outboxotel.InjectHeaders(ctx, msg.Headers) // W3C traceparent
...
worker, err := outbox.NewWorker(store, publisher, outbox.Config{Tracer: tracer})
```

The publish span is then a child of the enqueueing span. Tracing is optional
and the core package does not depend on OpenTelemetry.

### Operational questions

| Question | Where to look |
| --- | --- |
| How many events are waiting? | `pending_messages` gauge / `Stats.Pending` — exact at sample time |
| How old is the oldest pending event? | `oldest_pending_age_seconds` — **the backlog alert** |
| Are events being published? | `published_total{outcome="success"}` rate |
| How many retries are happening? | `retries_scheduled_total` rate |
| Are workers stuck? | `active_publishes` pinned at `Concurrency` with a flat success rate |
| Are leases expiring unexpectedly? | `expired_leases_total`, `lease_lost_total` — should be ~0 |
| Which event types are failing? | `failed_total{type,reason}` |
| Is the database or the broker the bottleneck? | Compare `claim_duration_seconds` and `store_errors_total` against `publish_duration_seconds` |

Costs: counters and histograms are in-process and cheap. The depth sample runs
three index scans plus one index lookup and is taken on `StatsInterval`, never
per message. Counts are exact as of the sample but their cost grows with the
number of rows in each state, so a very large failed or pending backlog makes
sampling more expensive — raise `StatsInterval` if that matters. Nothing on the
publishing path does a full table scan.

## Graceful shutdown

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := worker.Shutdown(ctx); err != nil {
	log.Error("outbox shutdown was not clean", "error", err)
}
```

`Shutdown` stops claiming immediately, then waits for in-flight publishes. If
its context expires first it cancels them and returns
`outbox.ErrShutdownTimeout`: those messages stay leased until their leases
expire and are then retried. Messages that were claimed but not yet handed to a
publisher are released at once, so they do not wait out a lease.

It is idempotent, safe to call concurrently, a no-op if the worker never
started, and it never records an unpublished message as published — the write
that records a success uses a context detached from shutdown so that a
successful publish is not turned into a duplicate by a cancellation.

**Your publisher must honour its context.** One that ignores cancellation can
keep running after `Shutdown` returns; the worker does not wait for it, because
that is the only way to keep shutdown bounded.

## Ordered delivery (optional)

```go
store, err := postgres.New(db, postgres.WithOrdering())
// then set Message.OrderingKey
```

A keyed message is not claimed while an earlier message with the same key is
unpublished. Precisely:

* Per key, at most one message is in flight under healthy leases, and
  message *n+1* does not start before *n* is marked published — so within a
  key, publication order follows enqueue order.
* **Per key only.** Nothing is guaranteed across keys, and there is no global
  ordering.
* It assumes your application serialises writes to one key (a row lock or a
  version check already does). Two concurrent transactions writing the same
  key can commit in an order that does not match their sequence numbers, and
  no outbox can see an uncommitted row.
* A permanently failed message **blocks its key** until an operator requeues
  or deletes it. That is the point of ordering, but it means ordered keys need
  failure alerting.
* An expired lease can still produce a duplicate, and that duplicate can
  arrive out of order relative to later messages for the same key.

It needs migration `0002`, which `Migrate` applies when the option is set.

## Throughput and backpressure

The database, not the library, is the limit. The worker's own overhead is a few
microseconds per message; a PostgreSQL round trip is hundreds of microseconds
to milliseconds, and each delivery costs one claim (amortised over the batch)
plus one acknowledging UPDATE, both of which must reach disk.

If producers outrun the publisher, the table grows and `available_at` falls
behind: the backlog is durable, bounded only by disk, and nothing is dropped or
buffered in memory. Watch `oldest_pending_age_seconds`; it rises long before
anything breaks. To go faster, in order of effect:

1. Raise `Concurrency` (and the connection pool with it) — acknowledgements
   are the per-message cost, and they parallelise.
2. Add worker replicas. `SKIP LOCKED` means they do not block each other.
3. Make sure your publisher is actually concurrent and not serialising on one
   connection or lock.
4. Consider `synchronous_commit = off` for the outbox database only if your
   durability budget allows it; acknowledgements are commit-bound.

See [docs/OPERATIONS.md](docs/OPERATIONS.md) for deployment, alerting and
tuning, [docs/ADR.md](docs/ADR.md) for the design decisions and the
alternatives that were rejected, and [docs/REPORT.md](docs/REPORT.md) for
measured benchmarks, test coverage and known limitations.

## Testing

```bash
go test ./...                  # unit tests; integration tests skip without a database
go test -race ./...
go vet ./...
go test -bench=. -benchmem ./...
```

Integration tests and database benchmarks need a PostgreSQL instance:

```bash
./scripts/pg.sh up             # starts one in Docker and prints the DSN
                               # OUTBOX_PG_IMAGE overrides the image
export OUTBOX_TEST_POSTGRES_DSN='postgres://outbox:outbox@127.0.0.1:55432/outbox?sslmode=disable'
go test -race ./...
./scripts/pg.sh down
```

They create and drop their own tables and are safe to run against a scratch
database. `make check` runs vet, unit tests and race tests across every module
in the repository.

The store uses only `database/sql`, and `postgres/pgxcompat` is a small
separate module that proves it by exercising the whole state machine through
`jackc/pgx/v5`'s driver instead of `lib/pq`. Neither driver is a dependency of
the library.

For your own tests, `outbox/memstore` is an in-memory store with the same state
machine, lease fencing, an injectable clock and fault injection:

```go
store := memstore.New()
store.SetFault(func(op string) error {
	if op == "mark_published" {
		return errors.New("database is away")
	}
	return nil
})
```

## Security notes

* All SQL is parameterised. The only interpolated identifier is the table name,
  which is validated against `^[a-zA-Z_][a-zA-Z0-9_]*$` per part at
  construction.
* Payloads are never logged and never used as metric labels.
* Stored error summaries are truncated to 512 bytes; override
  `Config.ErrorSummary` to redact errors that may embed credentials.
* Retention deletes only `published` rows, enforced in both the row selection
  and the `DELETE` itself.
* Retries, attempts, batch sizes and timeouts are all bounded and validated.

## License

MIT.
# gocast
