package outbox

import (
	"context"
	"errors"
)

// Sentinel errors returned by this package. All of them are safe to compare
// with [errors.Is]; errors that wrap a cause always preserve it.
var (
	// ErrInvalidMessage means a message failed validation and was not stored.
	ErrInvalidMessage = errors.New("outbox: invalid message")

	// ErrInvalidConfig means a worker configuration is not usable.
	ErrInvalidConfig = errors.New("outbox: invalid config")

	// ErrLeaseLost means a store update was rejected because the caller no
	// longer owns the message: its lease expired and another worker reclaimed
	// it, or an operator changed its state. The caller must not retry the
	// update; the new owner is responsible for the message.
	ErrLeaseLost = errors.New("outbox: lease lost")

	// ErrNotFound means no message exists with the given id.
	ErrNotFound = errors.New("outbox: message not found")

	// ErrWorkerRunning is returned by Worker.Start when the worker is already
	// running.
	ErrWorkerRunning = errors.New("outbox: worker already running")

	// ErrWorkerStopped is returned by Worker.Start after the worker has been
	// shut down. Workers are single-use.
	ErrWorkerStopped = errors.New("outbox: worker already stopped")

	// ErrShutdownTimeout means Shutdown's context expired before every
	// in-flight publish finished. In-flight publishes have been cancelled;
	// anything they did not finish stays leased until the lease expires and is
	// then retried by another worker.
	ErrShutdownTimeout = errors.New("outbox: shutdown timed out")
)

// PermanentError marks a publication failure that retrying cannot fix: a
// rejected schema, a 4xx from the broker, an unroutable topic. The worker moves
// such a message straight to the failed state without consuming its remaining
// retry budget.
type PermanentError struct{ Err error }

// Permanent wraps err so the worker treats it as non-retryable. It returns nil
// for a nil error.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// Error implements the error interface.
func (e *PermanentError) Error() string {
	if e.Err == nil {
		return "outbox: permanent failure"
	}
	return "outbox: permanent failure: " + e.Err.Error()
}

// Unwrap returns the underlying cause.
func (e *PermanentError) Unwrap() error { return e.Err }

// IsPermanent reports whether err was marked permanent with [Permanent].
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// Disposition is how the worker should treat a failed publication.
type Disposition int

const (
	// Retryable schedules another attempt after a backoff delay, until the
	// retry budget is exhausted.
	Retryable Disposition = iota
	// PermanentFailure moves the message to the failed state immediately.
	PermanentFailure
)

// String returns "permanent" or "retryable".
func (d Disposition) String() string {
	if d == PermanentFailure {
		return "permanent"
	}
	return "retryable"
}

// Classify is the default error classifier: errors marked with [Permanent] are
// permanent, everything else (including context deadlines, broker timeouts and
// network errors) is retryable.
//
// Context cancellation caused by shutdown is handled separately and never
// counts as a failure.
func Classify(err error) Disposition {
	if IsPermanent(err) {
		return PermanentFailure
	}
	return Retryable
}

// FailureReason explains why a message reached the failed state. It is a small
// closed set so it is safe to use as a metric label.
type FailureReason string

// The terminal failure reasons.
const (
	// ReasonPermanent means the publisher reported a non-retryable error.
	ReasonPermanent FailureReason = "permanent"
	// ReasonExhausted means the message ran out of delivery attempts.
	ReasonExhausted FailureReason = "exhausted"
	// ReasonUndecodable means the stored row could not be decoded, so it was
	// never handed to the publisher.
	ReasonUndecodable FailureReason = "undecodable"
)

// Failure is the summary the store records on a failed or rescheduled message.
// Error is already truncated and, if configured, redacted; it is meant for
// operators reading the table, not for programmatic matching.
type Failure struct {
	Kind  string
	Error string
}

// isContextError reports whether err is a context cancellation or deadline,
// including one reported by a driver that wrapped it.
func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// truncate shortens s to at most n bytes without splitting the final rune.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8Start(s[n]) {
		n--
	}
	return s[:n] + "…"
}

func utf8Start(b byte) bool { return b&0xc0 != 0x80 }

// defaultErrorSummary renders an error for storage: the full chain, truncated.
func defaultErrorSummary(err error) string {
	if err == nil {
		return ""
	}
	return truncate(err.Error(), 512)
}
