# Design decisions

Each section states the decision, the alternatives, and why they were rejected.

## 1. The store interface covers publishing only; enqueueing is adapter-specific

`outbox.Store` has six methods — `Claim`, `MarkPublished`, `Reschedule`,
`Fail`, `Release`, `Stats` — and no `Enqueue`. Enqueueing is a typed method on
the adapter (`postgres.Store.Enqueue`).

*Why.* Enqueueing has to accept the application's own transaction handle, whose
type the adapter knows and the core cannot. The alternatives were to put
`tx any` in the interface (unityped, unsafe) or to force every caller through a
`database/sql`-shaped abstraction even when they use `pgx` natively. Splitting
it costs nothing: the worker never enqueues, so the interface it depends on is
genuinely smaller. Applications already depend on their database concretely.

`postgres.Store.Enqueue` takes `outbox.Execer`, the one-method
`ExecContext` interface, so `*sql.Tx`, `*sql.DB` and `*sql.Conn` all work and
tests can substitute a fake. `EnqueueStatements` renders the same SQL for
drivers outside `database/sql`.

## 2. `FOR UPDATE SKIP LOCKED` in one statement, with a lease epoch as a fencing token

Claiming selects due rows with `FOR UPDATE SKIP LOCKED`, takes ownership, and
increments `attempts` and `lease_epoch` — all in a single statement. Every
later write is conditional on `(id, lease_epoch, state='leased')`.

*Why.* Selection and lease assignment must be atomic or two workers can claim
the same row; one statement makes that structural rather than careful.
`SKIP LOCKED` means concurrent workers step over each other's locked rows
instead of serialising behind them.

The epoch is what makes a *stale* worker harmless. A monotonic counter was
chosen over a random token because it is cheap, needs no extension
(`gen_random_uuid()` is PostgreSQL 13+), is readable in logs, and gives a
strict ordering: an update carrying an older epoch is provably from a previous
owner.

*Rejected:* advisory locks (lock state lives in the session, so it vanishes on
reconnect and cannot be inspected); a `claimed_by` column with no epoch (cannot
distinguish the current owner from a previous one that is still running);
`SELECT ... FOR UPDATE` without `SKIP LOCKED` (workers queue behind each other).

## 3. `AS MATERIALIZED` on both locking CTEs

The claim and retention statements both say `WITH ... AS MATERIALIZED`.

*Why.* This is not stylistic. The retention sweep was originally
`DELETE ... WHERE id IN (SELECT ... ORDER BY published_at LIMIT $n FOR UPDATE
SKIP LOCKED)`. PostgreSQL planned it as a nested-loop semi-join and
**re-executed the sub-select once per candidate row**, so a sweep with
`LIMIT 4` deleted every eligible row in the table — the limit was silently
ignored. An integration test caught it; `EXPLAIN ANALYZE` showed `loops=10` on
the sub-plan. Materialising the CTE pins it to exactly one evaluation.

The same hazard applies in principle to any locking sub-select whose row count
matters, so the claim statement is explicit about it too, and both carry a
comment saying why.

## 4. `available_at` means "when a worker may take this row", for pending and leased alike

There is no `lease_expires_at` column. For a pending row `available_at` is the
next attempt time; for a leased row it is the lease expiry.

*Why.* It collapses the claim predicate from two OR-ed branches into one. With
two branches, PostgreSQL cannot walk a single index in order, so it reads
*every* due row and sorts before applying the `LIMIT` — fine on a small table,
and quadratic-feeling on a large backlog. With one branch it walks one partial
index in order and stops at the limit. Measured on a 100k-row table with 60
unpublished rows: `Index Scan ... (actual rows=10)`, no sort node.

It also removes one index (the lease index), one `CHECK` constraint, and one
opportunity for the two timestamps to disagree. The cost is a column whose
meaning depends on `state`, which the schema comments and the README spell out.
`Claim` returns the pre-claim value as `Claimed.AvailableAt` so publication lag
is still measurable.

## 5. The database clock, never the worker's

Every stored timestamp comes from `now()` inside the statement. Store methods
take *durations* (`Lease`, `delay`), never absolute times.

*Why.* Lease expiry decides whether another worker may take a message. If each
worker used its own clock, a worker whose clock ran fast would reclaim
messages that are still legitimately owned, and one running slow would hold
expired leases. With one clock, skew is not part of the correctness argument.

## 6. The worker claims only as many messages as it has idle publishers

`min(BatchSize, idle goroutines)`, and the claimed batch is handed off over an
unbuffered channel.

*Why.* Nothing leased is parked in memory. The number of simultaneously leased
messages is at most `Concurrency`, a lease is held for at most one publish plus
its acknowledgement, and `PublishTimeout + StoreTimeout <= LeaseDuration`
becomes a checkable invariant instead of a guess. Backpressure is automatic: a
slow broker fills the publishers and the poller simply stops claiming.

*Rejected:* claiming a large batch into an in-memory queue. It amortises the
claim query, but the claim is not the bottleneck — the per-message
acknowledgement is — and it would make lease duration depend on queue wait,
which is unbounded in principle. Leased work sitting in memory also turns a
crash into a lease-expiry wait for the whole batch instead of for what was
actually in flight.

## 7. Polling, with an in-process `Notify` hint

A poller sleeps `PollInterval` (plus jitter) when it finds nothing, and polls
again immediately after a full batch. `Worker.Notify` wakes it early.

*Why.* Polling is the mechanism every other design needs as a fallback anyway,
and it has no failure mode worse than latency. `LISTEN/NOTIFY` adds a dedicated
connection, delivers nothing to a worker that was not listening at the time,
and still needs polling for durability. Logical decoding is a different
product. `Notify` recovers the latency for the common single-process case
without becoming load-bearing: correctness never depends on it, and
cross-process messages are found by the next poll.

## 8. `attempts` is incremented when a message is claimed, not when it fails

*Why.* A message that crashes its worker — a payload that panics a serialiser,
a size that kills the broker client — would otherwise be retried forever, and
each retry takes a worker with it. Counting at claim time bounds that: the
message consumes its budget and becomes `failed`, which is visible and
recoverable, instead of poisoning the deployment.

The cost is that a crash or an expired lease consumes an attempt that the
broker never saw, and that a graceful shutdown's released message has already
spent one. Both are visible (`expired_leases_total`, the `attempts` column) and
the `failed` state is recoverable with `Requeue`, which resets the budget.

## 9. Per-message acknowledgement, not batched

*Why.* Error attribution. A batched `UPDATE ... WHERE id IN (...)` cannot say
which message failed, and a partial failure forces either re-publishing the
whole batch or losing track. It is also the honest default: batching
acknowledgements widens the crash window in which an accepted message has not
been recorded, trading duplicates for throughput. The measured cost is one
commit per message, which parallelises across `Concurrency` and across
replicas. If a deployment needs more, the fix is more concurrency or more
replicas, both of which are free of correctness consequences.

## 10. UUIDv7 ids generated in the application

*Why.* Three properties at once: the id exists *before* the commit, so it can
be logged, returned to a caller and used as a broker deduplication key; it is
stable across every retry, which is what lets consumers deduplicate; and it is
time-ordered, so index inserts stay at the right-hand edge of the B-tree
instead of scattering as UUIDv4 does. Database-generated ids (`bigserial`,
`gen_random_uuid()`) are not known before the insert returns and are not
portable across adapters. `ParseID` lets an application supply its own
content-derived id when it wants enqueueing to be idempotent.

## 11. Observability behind interfaces, adapters in separate modules

The core defines `Observer` (metrics) and `Tracer` (spans) and ships
`NopObserver`. `contrib/prometheus` and `contrib/otel` are separate Go modules.

*Why.* The core module has no non-standard-library dependencies, so adopting it
adds nothing to anyone's dependency graph, and the choice of metrics library
stays the application's. The interfaces are explicit methods rather than a
generic `Counter(name string, labels map[string]string)` so that metric
cardinality is a property of the API rather than a convention: the only
unbounded-looking value any method receives is a message type, which is
documented, and ids, payloads and error strings are never passed at all.

Embedding `NopObserver` lets implementations survive new interface methods.

## 12. The failed state is terminal and manual

An exhausted or permanently failed message stops there. It is listable
(`ListFailed`) and recoverable (`Requeue`), and nothing deletes it.

*Why.* A dead-letter table would be a second place to look and a second thing
to migrate; the state column already separates terminal failures, and the
partial index makes listing them cheap. Automatic discarding is not on offer at
any setting: losing an event that was committed with a business change is the
one outcome the whole pattern exists to prevent.

## 13. Retention is opt-in and only ever deletes published rows

*Why.* Deleting is the only destructive thing the package can do, so it is off
unless asked for, restricted to the one state that is safe to remove, and
restricted in two places (the row selection and the `DELETE`) so that a mistake
in one cannot touch an undelivered message. Table partitioning was considered
and rejected: it only pays off at a scale where the retention window is
measured in days of millions of rows, and it would complicate the schema for
every user to serve a few.

## 14. Shutdown is bounded even if the publisher misbehaves

*Why.* `Shutdown(ctx)` stops claiming, waits for in-flight publishes until
`ctx` expires, then cancels them and returns. It does not then wait for the
goroutines: a publisher that ignores its context could otherwise hang shutdown
forever, which is worse than leaving a message leased — the lease expires and
another worker retries it.

The write that records a *successful* publish uses `context.WithoutCancel`
with its own timeout. Cancelling it would turn a delivered message into a
duplicate for no benefit, which is exactly the opposite of what a shutdown
deadline is for.

## 15. Rejected: an in-memory queue in front of the table

Buffering enqueued messages in memory and writing them in batches would make
enqueueing faster. It also breaks the single invariant the package exists to
provide: a message in memory is not part of the transaction, so a crash between
commit and flush loses an event that the business state says happened. The
table is the queue.
