# Running the outbox in production

## Sizing

Start here and adjust from measurements:

| Deployment | Concurrency | BatchSize | PollInterval | LeaseDuration |
| --- | --- | --- | --- | --- |
| Low volume (< 10/s) | 4 | 32 | 1s | 30s |
| Moderate (10–500/s) | 8–16 | 64 | 500ms | 30s |
| High (> 500/s) | 16–32 per replica, plus replicas | 64–128 | 100–250ms | 60s |
| Latency-sensitive | 8 | 32 | 100ms + `Notify()` after commit | 30s |

Rules that matter more than the numbers:

* **`PublishTimeout + StoreTimeout <= LeaseDuration`.** Enforced at
  construction. If you raise `PublishTimeout` for a slow broker, raise the
  lease with it.
* **The connection pool must exceed `Concurrency`.** Each in-flight publish
  needs a connection for its acknowledgement, plus one for the poller and one
  for stats sampling. `MaxOpenConns >= Concurrency + 2` for the worker's share,
  on top of whatever the application needs.
* **`LeaseDuration` is your crash-recovery delay.** After a worker dies, its
  in-flight messages wait out their leases. 30s is a reasonable compromise; go
  shorter only if `PublishTimeout` is also short.
* **Longer `PollInterval` saves queries, not much else.** An idle worker runs
  one cheap index lookup per interval. With four replicas at 100ms that is 40
  queries a second doing almost nothing; at 1s, four.

## Deployment

The worker is a goroutine inside your application, not a separate service. Run
it in every replica: `SKIP LOCKED` means they share the table without blocking
and without coordination. Nothing needs to elect a leader.

```go
worker, err := outbox.NewWorker(store, publisher, cfg)
if err != nil {
	return err
}
if err := worker.Start(ctx); err != nil {
	return err
}
defer func() {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := worker.Shutdown(shutdownCtx); err != nil {
		logger.Error("outbox shutdown was not clean", "error", err)
	}
}()
```

Give the shutdown deadline more room than `PublishTimeout` so in-flight work
can finish; otherwise every rolling deployment leaves a batch to be recovered
by lease expiry. Make sure your process's own termination grace period (for
example Kubernetes `terminationGracePeriodSeconds`) is longer than the
shutdown deadline, or the runtime will kill you mid-drain.

### Separating enqueue from publish

An application that only writes events can skip the worker entirely and let
other replicas publish. `postgres.Store` is safe to use for enqueueing with no
worker at all — useful for a write-heavy service plus a small dedicated
publisher deployment.

## Alerting

In rough order of usefulness:

| Alert | Condition | Means |
| --- | --- | --- |
| **Backlog age** | `oldest_pending_age_seconds > 60` for 5m | Publishing is falling behind. The single most useful alert |
| **Terminal failures** | `increase(outbox_failed_total[15m]) > 0` | Events are stuck and need a human. Page for ordered keys |
| **No progress with a backlog** | `pending_messages > 0` and `rate(published_total{outcome="success"}[5m]) == 0` | The worker is stuck, the broker is down, or the publisher is blocked |
| **Expired leases** | `rate(expired_leases_total[15m]) > 0` | Workers are dying or stalling mid-publish; duplicates are being produced |
| **Lost leases** | `rate(lease_lost_total[15m]) > 0` | Leases are too short for actual publish times. Raise `LeaseDuration` |
| **Store errors** | `rate(store_errors_total[5m]) > 0` | Database trouble. `operation="publish_ack"` is the one that causes duplicates |
| **Retry storm** | `rate(retries_scheduled_total[5m])` well above the publish rate | The broker is degraded; the backlog is about to grow |

Backlog age is the right primary signal because it is monotone: it rises as
soon as throughput drops below the enqueue rate, long before anything breaks,
and it is cheap to collect.

## Diagnosing a growing backlog

1. **Is the publisher slow or failing?** Compare
   `publish_duration_seconds` and the `outcome` breakdown. A high p99 with
   successes means the broker is slow; retryable failures mean it is unhealthy.
2. **Is the database the bottleneck?** `claim_duration_seconds` and
   `store_errors_total`. A slow claim on a large table usually means the
   partial indexes are missing or bloated — check with `EXPLAIN` that the claim
   uses `<table>_due_idx` and does **not** sort.
3. **Is the worker saturated?** `active_publishes` pinned at `Concurrency`
   means more concurrency or more replicas will help. Well below it with a
   backlog means the poller is starved — check `PollInterval` and the
   connection pool.
4. **Is one event type to blame?** `failed_total{type}` and
   `published_total{type,outcome}`.
5. **Is one ordering key blocking?**
   ```sql
   SELECT ordering_key, count(*), min(available_at)
     FROM outbox_messages
    WHERE state <> 'published' AND ordering_key IS NOT NULL
    GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
   ```
   A key with a `failed` head blocks everything behind it until you requeue or
   delete it.

### Draining faster

Raise `Concurrency` and the connection pool together, then add replicas. If
acknowledgements are the limit (they usually are — one commit per message),
the database's commit latency is the ceiling; `synchronous_commit = off` on the
outbox database trades durability for throughput and is a decision for whoever
owns that durability budget, not a default.

## Recovering failed messages

```sql
SELECT id, event_type, attempts, updated_at, last_error_kind, left(last_error, 200)
  FROM outbox_messages
 WHERE state = 'failed'
 ORDER BY seq
 LIMIT 100;
```

Or in Go, which is what tooling should use:

```go
failed, err := store.ListFailed(ctx, 100, outbox.ID{})
// ... fix the cause: deploy the consumer, create the topic, correct the schema
n, err := store.Requeue(ctx, ids...)
```

`Requeue` resets the attempt budget, clears the error and makes the message
available immediately. It only touches rows in the `failed` state, so it is
safe to call with a stale list of ids.

A message that is genuinely undeliverable — a payload no consumer will ever
accept — has to be deleted deliberately. Nothing in the package will do it for
you:

```sql
DELETE FROM outbox_messages WHERE state = 'failed' AND id = '...';
```

## Retention

Published rows accumulate until something removes them. Either enable the
built-in sweeper:

```go
outbox.Config{Retention: 7 * 24 * time.Hour, PruneInterval: 10 * time.Minute, PruneBatch: 1000}
```

or run your own job. If you do it yourself, keep the two guards the built-in
sweeper uses — a `state = 'published'` predicate and a `LIMIT` — and put the
limit in a **materialised** CTE:

```sql
WITH victims AS MATERIALIZED (
	SELECT id FROM outbox_messages
	 WHERE state = 'published' AND published_at < now() - interval '7 days'
	 ORDER BY published_at
	 LIMIT 1000
	   FOR UPDATE SKIP LOCKED
)
DELETE FROM outbox_messages m USING victims v
 WHERE m.id = v.id AND m.state = 'published';
```

Without `AS MATERIALIZED`, PostgreSQL may re-execute the sub-select per
candidate row and ignore the `LIMIT` entirely — see ADR 3. Never delete rows in
any other state.

Expect table and index bloat where deletes are frequent; autovacuum handles it,
but on a hot outbox consider a more aggressive per-table setting:

```sql
ALTER TABLE outbox_messages SET (autovacuum_vacuum_scale_factor = 0.02,
                                 autovacuum_analyze_scale_factor = 0.01);
```

## Schema changes and event evolution

Version the event type (`orders.created.v1` → `v2`) and let consumers accept
both during the transition. The outbox stores payloads as opaque bytes and
never parses them, so a payload change needs no migration here — but a change
that consumers cannot read will be published anyway, so deploy the consumer
first.

Adding a column to the outbox table is safe. Removing or renaming one that the
store's SQL mentions is not; pin the library version alongside such a change.

## Backups and replicas

The outbox table is ordinary data: it is covered by whatever backup and
replication the rest of the database has. Restoring a backup can republish
messages that were already delivered (the restored rows are pending again),
which is one more reason consumers must be idempotent.

Do not run a worker against a read replica: claiming writes.
