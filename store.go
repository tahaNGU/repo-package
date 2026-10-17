package outbox

import (
	"context"
	"database/sql"
	"time"
)

// Execer is the subset of [database/sql] used to write outbox rows. It is
// satisfied by *sql.Tx, *sql.DB and *sql.Conn.
//
// Pass a *sql.Tx — the same transaction that writes the business change — to
// get the outbox guarantee: the business rows and the outbox rows commit or
// roll back together. Passing a *sql.DB enqueues outside any transaction,
// which is occasionally useful (backfills, tests) but gives up that guarantee.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Store is the persistence contract the worker depends on. It covers only the
// publishing side; enqueueing is a typed, adapter-specific call because it has
// to accept the application's own transaction handle (see
// [github.com/tahaNGU/gocast/postgres.Store.Enqueue]).
//
// Implementations must satisfy three rules, which is what makes concurrent
// workers safe:
//
//  1. Claim is atomic. Two concurrent Claim calls never return the same
//     message, and claiming a message assigns its lease in the same operation.
//  2. Every mutation is fenced. MarkPublished, Reschedule, Fail and Release
//     apply only if the message is still leased at exactly the given
//     leaseEpoch; otherwise they report [ErrLeaseLost] and change nothing.
//  3. Timestamps come from the store's clock, not the caller's, so that
//     workers with skewed clocks cannot mis-expire each other's leases.
type Store interface {
	// Claim atomically leases up to req.Limit messages that are due, plus
	// messages whose lease has expired, and increments their attempt count and
	// lease epoch.
	Claim(ctx context.Context, req ClaimRequest) (ClaimResult, error)

	// MarkPublished records successful publication. It is the only transition
	// that may not be guessed: callers must never call it unless the publisher
	// returned nil.
	MarkPublished(ctx context.Context, id ID, leaseEpoch int64) error

	// Reschedule returns the message to the pending state, available after
	// delay, and records the failure summary. The attempt count is not changed
	// (it was already incremented at claim time).
	Reschedule(ctx context.Context, id ID, leaseEpoch int64, delay time.Duration, f Failure) error

	// Fail moves the message to the terminal failed state with a summary.
	Fail(ctx context.Context, id ID, leaseEpoch int64, f Failure) error

	// Release returns the message to the pending state, available
	// immediately, without recording a failure. The worker uses it when a
	// publication was abandoned rather than failed, for example during
	// shutdown.
	Release(ctx context.Context, id ID, leaseEpoch int64) error

	// Stats samples queue depth and lag. It is called on an interval, never
	// per message.
	Stats(ctx context.Context) (Stats, error)
}

// ClaimRequest asks the store to lease work.
type ClaimRequest struct {
	// Owner identifies the claiming worker. It is stored for operators and is
	// not used for correctness; fencing uses the lease epoch.
	Owner string
	// Limit is the maximum number of messages to lease; always >= 1.
	Limit int
	// Lease is how long the claim is valid for.
	Lease time.Duration
}

// ClaimResult is the outcome of a claim.
type ClaimResult struct {
	// Messages are the leased messages, ordered by sequence number.
	Messages []Claimed
	// ExpiredLeases counts how many of Messages were taken over from a lease
	// that had expired. A non-zero value means earlier attempts did not finish
	// cleanly: a worker crashed, stalled, or lost the database.
	ExpiredLeases int
}

// Pruner is an optional [Store] capability: deleting published messages that
// are older than a retention window. A store that implements it can be driven
// by the worker's retention sweeper (see [Config.Retention]).
//
// Implementations must only ever delete rows in the published state.
type Pruner interface {
	// DeletePublished removes at most limit messages published longer than
	// olderThan ago and reports how many it deleted.
	DeletePublished(ctx context.Context, olderThan time.Duration, limit int) (int, error)
}

// Inspector is an optional [Store] capability for operator tooling: listing
// and requeueing terminally failed messages.
type Inspector interface {
	// ListFailed returns failed messages, oldest first, starting after the
	// given id (pass the zero ID for the first page).
	ListFailed(ctx context.Context, limit int, after ID) ([]FailedMessage, error)
	// Requeue returns failed messages to the pending state with a fresh
	// attempt budget and reports how many were requeued. Messages that are not
	// in the failed state are left untouched.
	Requeue(ctx context.Context, ids ...ID) (int, error)
}
