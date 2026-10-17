// Package memstore provides an in-memory [outbox.Store] for tests.
//
// It implements the same state machine, lease fencing and claim atomicity as
// the Postgres store, so worker behaviour can be exercised without a database.
// It also supports an injectable clock and fault injection, which makes crash
// and failure windows reproducible.
//
// It is not a substitute for the real adapter: it cannot join an application's
// SQL transaction, and it verifies nothing about SQL-level locking. Use it for
// unit tests, and the Postgres store for integration tests.
package memstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/tahaNGU/gocast"
)

// Store is an in-memory outbox. It is safe for concurrent use. The zero value
// is not usable; call [New].
type Store struct {
	mu   sync.Mutex
	rows map[outbox.ID]*row
	seq  int64

	clock func() time.Time
	fault func(op string) error
}

type row struct {
	msg         outbox.Message
	seq         int64
	state       outbox.State
	attempts    int
	availableAt time.Time
	createdAt   time.Time
	publishedAt time.Time
	leaseEpoch  int64
	leaseOwner  string
	leaseUntil  time.Time
	lastError   string
	errorKind   string
	// corrupt makes a claim of this row report a decode error, which models a
	// row whose stored headers cannot be parsed.
	corrupt bool
}

// New returns an empty in-memory store.
func New() *Store {
	return &Store{rows: make(map[outbox.ID]*row), clock: func() time.Time { return time.Now().UTC() }}
}

// SetClock replaces the store's clock, so tests can move time forward without
// sleeping. The function must be safe for concurrent use.
func (s *Store) SetClock(fn func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock = fn
}

// SetFault installs a hook consulted at the start of every store operation.
// Returning a non-nil error makes that operation fail, which models a database
// outage. Pass nil to clear it. Operation names are "enqueue", "claim",
// "mark_published", "reschedule", "fail", "release", "stats", "prune",
// "list_failed" and "requeue".
func (s *Store) SetFault(fn func(op string) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = fn
}

// Corrupt marks a stored message so that claiming it reports a decode error,
// modelling an unreadable row.
func (s *Store) Corrupt(id outbox.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.rows[id]; ok {
		r.corrupt = true
	}
}

// Tx is a fake transaction. Messages enqueued on it become visible only when
// it is committed, which is what lets tests assert the outbox invariant:
// business state and outbox rows commit or roll back together.
type Tx struct {
	s      *Store
	mu     sync.Mutex
	staged []outbox.Message
	closed bool
}

// Begin starts a fake transaction.
func (s *Store) Begin() *Tx { return &Tx{s: s} }

// Commit makes the staged messages visible.
func (t *Tx) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errors.New("memstore: transaction already closed")
	}
	t.closed = true
	t.s.insert(t.staged...)
	t.staged = nil
	return nil
}

// Rollback discards the staged messages. Calling it after Commit is a no-op,
// mirroring the usual `defer tx.Rollback()` pattern.
func (t *Tx) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	t.staged = nil
	return nil
}

// Enqueue stages messages on tx. Pass a nil tx to insert immediately.
func (s *Store) Enqueue(ctx context.Context, tx *Tx, msgs ...outbox.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkFault("enqueue"); err != nil {
		return err
	}
	prepared := make([]outbox.Message, 0, len(msgs))
	for _, m := range msgs {
		if err := m.Validate(); err != nil {
			return err
		}
		if m.ID.IsZero() {
			m.ID = outbox.NewID()
		}
		prepared = append(prepared, m)
	}
	if tx == nil {
		s.insert(prepared...)
		return nil
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return errors.New("memstore: transaction already closed")
	}
	tx.staged = append(tx.staged, prepared...)
	return nil
}

func (s *Store) insert(msgs ...outbox.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	for _, m := range msgs {
		s.seq++
		available := m.AvailableAt
		if available.IsZero() || available.Before(now) {
			available = now
		}
		m.CreatedAt = now
		s.rows[m.ID] = &row{
			msg:         m,
			seq:         s.seq,
			state:       outbox.StatePending,
			availableAt: available.UTC(),
			createdAt:   now,
		}
	}
}

// Claim implements [outbox.Store].
func (s *Store) Claim(ctx context.Context, req outbox.ClaimRequest) (outbox.ClaimResult, error) {
	if err := ctx.Err(); err != nil {
		return outbox.ClaimResult{}, err
	}
	if err := s.checkFault("claim"); err != nil {
		return outbox.ClaimResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()

	due := make([]*row, 0, req.Limit)
	for _, r := range s.rows {
		switch {
		case r.state == outbox.StatePending && !r.availableAt.After(now):
			due = append(due, r)
		case r.state == outbox.StateLeased && !r.leaseUntil.After(now):
			due = append(due, r)
		}
	}
	slices.SortFunc(due, func(a, b *row) int {
		if a.availableAt.Equal(b.availableAt) {
			return int(a.seq - b.seq)
		}
		if a.availableAt.Before(b.availableAt) {
			return -1
		}
		return 1
	})
	if len(due) > req.Limit {
		due = due[:req.Limit]
	}

	var res outbox.ClaimResult
	res.Messages = make([]outbox.Claimed, 0, len(due))
	for _, r := range due {
		reclaimed := r.state == outbox.StateLeased
		if reclaimed {
			res.ExpiredLeases++
		}
		r.state = outbox.StateLeased
		r.attempts++
		r.leaseEpoch++
		r.leaseOwner = req.Owner
		r.leaseUntil = now.Add(req.Lease)
		c := outbox.Claimed{
			Message:        r.msg,
			Seq:            r.seq,
			Attempt:        r.attempts,
			LeaseEpoch:     r.leaseEpoch,
			LeaseExpiresAt: r.leaseUntil,
			ReclaimedLease: reclaimed,
		}
		c.Message.AvailableAt = r.availableAt
		c.Message.CreatedAt = r.createdAt
		if r.corrupt {
			c.DecodeErr = fmt.Errorf("memstore: simulated decode failure for %s", r.msg.ID)
			c.Message.Headers = nil
		}
		res.Messages = append(res.Messages, c)
	}
	return res, nil
}

// MarkPublished implements [outbox.Store].
func (s *Store) MarkPublished(ctx context.Context, id outbox.ID, leaseEpoch int64) error {
	return s.transition(ctx, "mark_published", id, leaseEpoch, func(r *row, now time.Time) {
		r.state = outbox.StatePublished
		r.publishedAt = now
		r.leaseOwner = ""
		r.leaseUntil = time.Time{}
	})
}

// Reschedule implements [outbox.Store].
func (s *Store) Reschedule(ctx context.Context, id outbox.ID, leaseEpoch int64, delay time.Duration, f outbox.Failure) error {
	return s.transition(ctx, "reschedule", id, leaseEpoch, func(r *row, now time.Time) {
		r.state = outbox.StatePending
		r.availableAt = now.Add(delay)
		r.lastError = f.Error
		r.errorKind = f.Kind
		r.leaseOwner = ""
		r.leaseUntil = time.Time{}
	})
}

// Fail implements [outbox.Store].
func (s *Store) Fail(ctx context.Context, id outbox.ID, leaseEpoch int64, f outbox.Failure) error {
	return s.transition(ctx, "fail", id, leaseEpoch, func(r *row, now time.Time) {
		r.state = outbox.StateFailed
		r.lastError = f.Error
		r.errorKind = f.Kind
		r.leaseOwner = ""
		r.leaseUntil = time.Time{}
	})
}

// Release implements [outbox.Store].
func (s *Store) Release(ctx context.Context, id outbox.ID, leaseEpoch int64) error {
	return s.transition(ctx, "release", id, leaseEpoch, func(r *row, now time.Time) {
		r.state = outbox.StatePending
		r.availableAt = now
		r.leaseOwner = ""
		r.leaseUntil = time.Time{}
	})
}

func (s *Store) transition(ctx context.Context, op string, id outbox.ID, leaseEpoch int64, apply func(*row, time.Time)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkFault(op); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return fmt.Errorf("%w: %s", outbox.ErrNotFound, id)
	}
	if r.state != outbox.StateLeased || r.leaseEpoch != leaseEpoch {
		return fmt.Errorf("%w: %s (state %s, epoch %d, wanted epoch %d)",
			outbox.ErrLeaseLost, id, r.state, r.leaseEpoch, leaseEpoch)
	}
	apply(r, s.clock())
	return nil
}

// Stats implements [outbox.Store].
func (s *Store) Stats(ctx context.Context) (outbox.Stats, error) {
	if err := ctx.Err(); err != nil {
		return outbox.Stats{}, err
	}
	if err := s.checkFault("stats"); err != nil {
		return outbox.Stats{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	st := outbox.Stats{ObservedAt: now}
	var oldest time.Time
	for _, r := range s.rows {
		switch r.state {
		case outbox.StatePending:
			st.Pending++
			if !r.availableAt.After(now) && (oldest.IsZero() || r.availableAt.Before(oldest)) {
				oldest = r.availableAt
			}
		case outbox.StateLeased:
			st.Leased++
		case outbox.StateFailed:
			st.Failed++
		}
	}
	if !oldest.IsZero() {
		st.OldestPendingAge = now.Sub(oldest)
	}
	return st, nil
}

// DeletePublished implements [outbox.Pruner].
func (s *Store) DeletePublished(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.checkFault("prune"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.clock().Add(-olderThan)
	var n int
	for id, r := range s.rows {
		if n >= limit {
			break
		}
		if r.state == outbox.StatePublished && r.publishedAt.Before(cutoff) {
			delete(s.rows, id)
			n++
		}
	}
	return n, nil
}

// ListFailed implements [outbox.Inspector].
func (s *Store) ListFailed(ctx context.Context, limit int, after outbox.ID) ([]outbox.FailedMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.checkFault("list_failed"); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([]*row, 0)
	for _, r := range s.rows {
		if r.state == outbox.StateFailed {
			rows = append(rows, r)
		}
	}
	slices.SortFunc(rows, func(a, b *row) int { return int(a.seq - b.seq) })
	out := make([]outbox.FailedMessage, 0, limit)
	skipping := !after.IsZero()
	for _, r := range rows {
		if skipping {
			if r.msg.ID == after {
				skipping = false
			}
			continue
		}
		if len(out) >= limit {
			break
		}
		out = append(out, outbox.FailedMessage{
			ID:          r.msg.ID,
			Type:        r.msg.Type,
			Attempts:    r.attempts,
			LastError:   r.lastError,
			ErrorKind:   r.errorKind,
			Aggregate:   r.msg.AggregateType + "/" + r.msg.AggregateID,
			OrderingKey: r.msg.OrderingKey,
		})
	}
	return out, nil
}

// Requeue implements [outbox.Inspector].
func (s *Store) Requeue(ctx context.Context, ids ...outbox.ID) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.checkFault("requeue"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	var n int
	for _, id := range ids {
		r, ok := s.rows[id]
		if !ok || r.state != outbox.StateFailed {
			continue
		}
		r.state = outbox.StatePending
		r.attempts = 0
		r.availableAt = now
		r.lastError = ""
		r.errorKind = ""
		n++
	}
	return n, nil
}

func (s *Store) checkFault(op string) error {
	s.mu.Lock()
	fn := s.fault
	s.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(op)
}

// Snapshot is the observable state of a stored message, for assertions.
type Snapshot struct {
	ID          outbox.ID
	Type        string
	State       outbox.State
	Attempts    int
	LeaseEpoch  int64
	AvailableAt time.Time
	PublishedAt time.Time
	LastError   string
	ErrorKind   string
}

// Get returns a snapshot of one message.
func (s *Store) Get(id outbox.ID) (Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return Snapshot{}, false
	}
	return r.snapshot(), true
}

// All returns snapshots of every stored message, ordered by enqueue sequence.
func (s *Store) All() []Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Snapshot, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, r.snapshot())
	}
	slices.SortFunc(out, func(a, b Snapshot) int { return a.AvailableAt.Compare(b.AvailableAt) })
	return out
}

// Len returns the number of stored messages.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

// CountState returns how many messages are in the given state.
func (s *Store) CountState(state outbox.State) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, r := range s.rows {
		if r.state == state {
			n++
		}
	}
	return n
}

func (r *row) snapshot() Snapshot {
	return Snapshot{
		ID:          r.msg.ID,
		Type:        r.msg.Type,
		State:       r.state,
		Attempts:    r.attempts,
		LeaseEpoch:  r.leaseEpoch,
		AvailableAt: r.availableAt,
		PublishedAt: r.publishedAt,
		LastError:   r.lastError,
		ErrorKind:   r.errorKind,
	}
}

var (
	_ outbox.Store     = (*Store)(nil)
	_ outbox.Pruner    = (*Store)(nil)
	_ outbox.Inspector = (*Store)(nil)
)
