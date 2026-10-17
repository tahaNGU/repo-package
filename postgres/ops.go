package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tahaNGU/gocast"
)

// Claim implements [outbox.Store].
//
// It runs one statement that selects due rows with FOR UPDATE SKIP LOCKED,
// takes ownership of them, and returns them. Because the selection and the
// lease assignment are the same statement, two workers can never hold the same
// message: a row already locked by a concurrent claim is skipped rather than
// waited for, so workers do not serialise behind each other.
//
// Rows whose lease has expired are picked up by the same statement, which also
// advances their lease epoch and so fences the previous owner. No separate
// recovery pass is needed.
func (s *Store) Claim(ctx context.Context, req outbox.ClaimRequest) (outbox.ClaimResult, error) {
	if req.Limit <= 0 {
		return outbox.ClaimResult{}, fmt.Errorf("outbox/postgres: claim limit must be positive, got %d", req.Limit)
	}
	if req.Lease <= 0 {
		return outbox.ClaimResult{}, fmt.Errorf("outbox/postgres: claim lease must be positive, got %s", req.Lease)
	}
	rows, err := s.db.QueryContext(ctx, s.claimSQL, req.Limit, req.Owner, req.Lease.Seconds())
	if err != nil {
		return outbox.ClaimResult{}, fmt.Errorf("outbox/postgres: claim: %w", err)
	}
	defer rows.Close()

	res := outbox.ClaimResult{Messages: make([]outbox.Claimed, 0, req.Limit)}
	for rows.Next() {
		var (
			c           outbox.Claimed
			headers     string
			contentType sql.NullString
			orderingKey sql.NullString
			aggType     sql.NullString
			aggID       sql.NullString
			leaseUntil  time.Time
			prevState   string
		)
		// AvailableAt is the value from before the claim: when the message
		// became due. The new value is the lease expiry.
		if err := rows.Scan(
			&c.ID, &c.Type, &c.Payload, &contentType, &headers,
			&aggType, &aggID, &orderingKey,
			&c.CreatedAt, &c.AvailableAt, &c.Attempt, &c.Seq,
			&c.LeaseEpoch, &leaseUntil, &prevState,
		); err != nil {
			return res, fmt.Errorf("outbox/postgres: scan claimed message: %w", err)
		}
		c.ContentType = contentType.String
		c.AggregateType = aggType.String
		c.AggregateID = aggID.String
		c.OrderingKey = orderingKey.String
		c.LeaseExpiresAt = leaseUntil.UTC()
		c.CreatedAt = c.CreatedAt.UTC()
		c.AvailableAt = c.AvailableAt.UTC()
		if headers != "" && headers != "{}" {
			if err := json.Unmarshal([]byte(headers), &c.Headers); err != nil {
				// The row is unreadable rather than undeliverable. Report it
				// so the worker can fail it permanently instead of handing a
				// half-built message to the publisher.
				c.Headers = nil
				c.DecodeErr = fmt.Errorf("outbox/postgres: headers of %s are not valid JSON: %w", c.ID, err)
			}
		}
		if prevState == string(outbox.StateLeased) {
			c.ReclaimedLease = true
			res.ExpiredLeases++
		}
		res.Messages = append(res.Messages, c)
	}
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("outbox/postgres: claim: %w", err)
	}
	return res, nil
}

// MarkPublished implements [outbox.Store].
func (s *Store) MarkPublished(ctx context.Context, id outbox.ID, leaseEpoch int64) error {
	return s.fenced(ctx, "mark published", s.publishSQL, id, leaseEpoch)
}

// Reschedule implements [outbox.Store].
func (s *Store) Reschedule(ctx context.Context, id outbox.ID, leaseEpoch int64, delay time.Duration, f outbox.Failure) error {
	if delay < 0 {
		delay = 0
	}
	return s.fenced(ctx, "reschedule", s.rescheduleSQL, id, leaseEpoch,
		delay.Seconds(), nullString(f.Error), nullString(f.Kind))
}

// Fail implements [outbox.Store].
func (s *Store) Fail(ctx context.Context, id outbox.ID, leaseEpoch int64, f outbox.Failure) error {
	return s.fenced(ctx, "fail", s.failSQL, id, leaseEpoch, nullString(f.Error), nullString(f.Kind))
}

// Release implements [outbox.Store].
func (s *Store) Release(ctx context.Context, id outbox.ID, leaseEpoch int64) error {
	return s.fenced(ctx, "release", s.releaseSQL, id, leaseEpoch)
}

// fenced runs a statement whose WHERE clause requires the message to still be
// leased at leaseEpoch, and turns "no rows changed" into a precise error.
func (s *Store) fenced(ctx context.Context, op, query string, id outbox.ID, leaseEpoch int64, extra ...any) error {
	args := append([]any{id, leaseEpoch}, extra...)
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("outbox/postgres: %s %s: %w", op, id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("outbox/postgres: %s %s: %w", op, id, err)
	}
	if n == 1 {
		return nil
	}
	return s.explainFenceMiss(ctx, op, id, leaseEpoch)
}

// explainFenceMiss turns a rejected update into an error that says why, which
// costs one extra query but only on a path that is already exceptional.
func (s *Store) explainFenceMiss(ctx context.Context, op string, id outbox.ID, leaseEpoch int64) error {
	var (
		state string
		epoch int64
	)
	err := s.db.QueryRowContext(ctx, s.stateSQL, id).Scan(&state, &epoch)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("outbox/postgres: %s: %w: %s", op, outbox.ErrNotFound, id)
	case err != nil:
		// The update was rejected and we cannot say why; report it as a lost
		// lease, which is the safe interpretation: the caller stops touching
		// the message.
		return fmt.Errorf("outbox/postgres: %s %s: %w: could not read current state: %w",
			op, id, outbox.ErrLeaseLost, err)
	default:
		return fmt.Errorf("outbox/postgres: %s %s: %w: state is %s at epoch %d, held epoch %d",
			op, id, outbox.ErrLeaseLost, state, epoch, leaseEpoch)
	}
}

// Stats implements [outbox.Store].
//
// Counts are exact as of the query, and each one is an index-only scan over a
// partial index, so the cost grows with the number of rows in that state, not
// with the size of the table. Published rows are never scanned. Treat it as a
// sampling query: the worker runs it on Config.StatsInterval, never per
// message.
func (s *Store) Stats(ctx context.Context) (outbox.Stats, error) {
	var (
		st     outbox.Stats
		oldest sql.NullTime
	)
	err := s.db.QueryRowContext(ctx, s.statsSQL).
		Scan(&st.Pending, &st.Leased, &st.Failed, &oldest, &st.ObservedAt)
	if err != nil {
		return outbox.Stats{}, fmt.Errorf("outbox/postgres: stats: %w", err)
	}
	st.ObservedAt = st.ObservedAt.UTC()
	if oldest.Valid {
		if age := st.ObservedAt.Sub(oldest.Time.UTC()); age > 0 {
			st.OldestPendingAge = age
		}
	}
	return st, nil
}

// DeletePublished implements [outbox.Pruner]. It only ever deletes rows in the
// published state: the statement restricts on that state both in the subquery
// that picks rows and in the DELETE itself, and it skips rows another
// transaction is touching.
func (s *Store) DeletePublished(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	if olderThan <= 0 {
		return 0, fmt.Errorf("outbox/postgres: retention must be positive, got %s", olderThan)
	}
	if limit <= 0 {
		return 0, fmt.Errorf("outbox/postgres: delete limit must be positive, got %d", limit)
	}
	res, err := s.db.ExecContext(ctx, s.pruneSQL, olderThan.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("outbox/postgres: delete published: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("outbox/postgres: delete published: %w", err)
	}
	return int(n), nil
}

// ListFailed implements [outbox.Inspector]. Results are ordered by enqueue
// sequence; pass the last id of a page as after to get the next one.
func (s *Store) ListFailed(ctx context.Context, limit int, after outbox.ID) ([]outbox.FailedMessage, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("outbox/postgres: list limit must be positive, got %d", limit)
	}
	var afterArg any
	if !after.IsZero() {
		afterArg = after
	}
	rows, err := s.db.QueryContext(ctx, s.listFailedSQL, limit, afterArg)
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: list failed: %w", err)
	}
	defer rows.Close()

	out := make([]outbox.FailedMessage, 0, limit)
	for rows.Next() {
		var (
			m              outbox.FailedMessage
			aggType, aggID string
		)
		if err := rows.Scan(&m.ID, &m.Type, &m.Attempts, &m.FailedAt,
			&m.LastError, &m.ErrorKind, &aggType, &aggID, &m.OrderingKey); err != nil {
			return out, fmt.Errorf("outbox/postgres: scan failed message: %w", err)
		}
		m.FailedAt = m.FailedAt.UTC()
		if aggType != "" || aggID != "" {
			m.Aggregate = aggType + "/" + aggID
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("outbox/postgres: list failed: %w", err)
	}
	return out, nil
}

// Requeue implements [outbox.Inspector]. Only messages in the failed state are
// affected, and they come back with a fresh attempt budget and no lease, so a
// worker picks them up on its next poll. Messages that are pending, leased or
// published are left alone.
func (s *Store) Requeue(ctx context.Context, ids ...outbox.ID) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	var b strings.Builder
	b.WriteString(`UPDATE `)
	b.WriteString(s.table)
	b.WriteString(` SET state = 'pending', attempts = 0, available_at = now(), updated_at = now(),
       published_at = NULL, lease_owner = NULL,
       last_error = NULL, last_error_kind = NULL
 WHERE state = 'failed' AND id IN (`)
	args := make([]any, 0, len(ids))
	for i, id := range ids {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(i + 1))
		b.WriteString("::uuid")
		args = append(args, id)
	}
	b.WriteByte(')')

	res, err := s.db.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("outbox/postgres: requeue: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("outbox/postgres: requeue: %w", err)
	}
	return int(n), nil
}

var (
	_ outbox.Store     = (*Store)(nil)
	_ outbox.Pruner    = (*Store)(nil)
	_ outbox.Inspector = (*Store)(nil)
)
