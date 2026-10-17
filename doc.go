// Package outbox implements the transactional outbox pattern: a way to change
// business state and emit an event without the two ever disagreeing.
//
// # The problem
//
// A service that writes to its database and then publishes to a broker has no
// way to make those two writes atomic. If the publish fails, the state change
// is already committed and the event is lost. If the publish succeeds and the
// transaction then rolls back, the event describes something that never
// happened. Retrying around the edges turns one race into several.
//
// The outbox pattern removes the race by writing the event into the same
// database, in the same transaction, as the business change. A background
// worker reads those rows afterwards and publishes them. The database is the
// only thing that has to be atomic, and it already is.
//
// # Two halves
//
// Enqueueing is synchronous and transactional. It is an INSERT on the caller's
// own *sql.Tx, so it commits or rolls back with the business change and costs
// no network round trip to any broker:
//
//	tx, err := db.BeginTx(ctx, nil)
//	if err != nil {
//		return err
//	}
//	defer tx.Rollback()
//
//	if err := placeOrder(ctx, tx, order); err != nil {
//		return err
//	}
//	err = store.Enqueue(ctx, tx, outbox.Message{
//		Type:        "orders.created.v1",
//		Payload:     body,
//		ContentType: "application/json",
//		AggregateType: "order",
//		AggregateID:   order.ID,
//	})
//	if err != nil {
//		return err
//	}
//	return tx.Commit()
//
// Publishing is asynchronous. A [Worker] leases batches of stored messages,
// hands each to a [Publisher], and records the outcome:
//
//	worker, err := outbox.NewWorker(store, publisher, outbox.Config{})
//	if err != nil {
//		return err
//	}
//	if err := worker.Start(ctx); err != nil {
//		return err
//	}
//	defer func() {
//		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//		defer cancel()
//		if err := worker.Shutdown(ctx); err != nil {
//			log.Error("outbox shutdown", "error", err)
//		}
//	}()
//
// A broker call never happens inside a database transaction, and no message
// is held in memory as the authoritative copy: the table is the queue.
//
// # Delivery guarantees
//
// What this package guarantees:
//
//   - A message committed with its business transaction is eventually
//     published or ends in the failed state. It is never silently dropped.
//   - A message that was not committed is never published.
//   - Under healthy leases, at most one worker owns a message at a time.
//   - Every state change is fenced by a lease epoch, so a worker that lost
//     its lease cannot overwrite the state written by the worker that took
//     over.
//   - Retry schedules are persisted, so restarts and crashes do not reset
//     them.
//
// What it does not guarantee:
//
//   - Exactly-once delivery. Delivery is at-least-once, and duplicates are
//     unavoidable in a specific window: if the broker accepts a message and
//     the worker dies before recording that fact, the only two choices are to
//     publish again (a duplicate) or to drop it (a loss). This package
//     chooses the duplicate. Removing that window would require the broker
//     and the database to commit together — a distributed transaction, or an
//     idempotency key enforced by the consumer. Consumers must therefore be
//     idempotent; [Message.ID] is stable across every attempt and is the
//     natural deduplication key.
//   - Global ordering. Messages are claimed oldest-first, but they are
//     published concurrently, retried on their own schedules, and may be
//     delivered out of order. Opt into per-key serialisation with the
//     store's ordering option if you need it, and read the exact guarantee
//     documented there.
//   - Delivery to the broker's satisfaction. The package records what the
//     [Publisher] reports. If a publisher returns nil without the broker
//     having durably accepted the message, the outbox will believe it.
//
// # Failure windows, explicitly
//
//	Business transaction rolls back    No outbox row exists; nothing is published.
//	Commit succeeds, process dies      The row is committed; another worker publishes it.
//	Worker dies after claiming         The lease expires; another worker retries. One attempt is consumed.
//	Broker accepts, worker dies        The lease expires and the message is published again: a duplicate.
//	Lease expires mid-publish          Another worker may publish it too. The stale worker's writes are rejected.
//	Two workers claim concurrently     Impossible: claiming is a single atomic statement.
//	Stale worker writes after reclaim  Rejected with [ErrLeaseLost]; the new owner's state stands.
//	Database unavailable               The worker backs off and retries; nothing is lost or acknowledged.
//	Broker returns a permanent error   The message goes straight to the failed state, inspectable and requeueable.
//	Shutdown during publication        In-flight work is awaited, then cancelled and released for retry.
//
// # Package layout
//
//	outbox                      this package: model, interfaces, worker
//	outbox/postgres             the reference store, SQL and migrations
//	outbox/memstore             an in-memory store for tests
//	outbox/contrib/prometheus   metrics (separate module)
//	outbox/contrib/otel         tracing (separate module)
//
// The core package depends only on the standard library.
package outbox
