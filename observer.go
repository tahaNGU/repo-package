package outbox

import (
	"context"
	"time"
)

// Observer receives the runtime signals the worker produces. It exists so the
// core package can be fully instrumented without depending on any metrics
// library; see github.com/tahaNGU/gocast/contrib/prometheus for a ready
// implementation, or implement the three or four methods you care about and
// embed [NopObserver] for the rest.
//
// Implementations must be safe for concurrent use and must not block: they are
// called from the polling and publishing paths.
//
// Cardinality: the only unbounded-looking value passed to an Observer is a
// message type. Keep [Message.Type] a small, versioned set if you turn it into
// a metric label. Message ids, aggregate ids, error strings and payloads are
// never passed to an Observer.
type Observer interface {
	// ClaimCompleted reports the result of one claim query: how many messages
	// were leased, how many of those were taken over from expired leases, and
	// how long the query took. A claim that returned no work still reports.
	ClaimCompleted(ctx context.Context, claimed, expiredLeases int, d time.Duration)

	// PublishCompleted reports one delivery attempt: its type, outcome,
	// attempt number and the time spent inside Publisher.Publish.
	PublishCompleted(ctx context.Context, e PublishEvent)

	// RetryScheduled reports that a message will be retried after delay.
	RetryScheduled(ctx context.Context, msgType string, attempt int, delay time.Duration)

	// MessageFailed reports that a message reached the terminal failed state.
	MessageFailed(ctx context.Context, msgType string, reason FailureReason, attempts int)

	// BatchCompleted reports that a claimed batch of the given size finished
	// processing, and how long that took end to end.
	BatchCompleted(ctx context.Context, size int, d time.Duration)

	// StoreFailed reports a failed store operation ("claim", "publish_ack",
	// "reschedule", "fail", "release", "stats", "prune"). The error itself is
	// logged, not passed here, to keep metric labels bounded.
	StoreFailed(ctx context.Context, op string)

	// LeaseLost reports that a fenced update was rejected because the message
	// had been reclaimed by another worker. Non-zero values mean leases are
	// expiring while publishers are still running: raise LeaseDuration or
	// lower PublishTimeout.
	LeaseLost(ctx context.Context, op string)

	// WorkersActive reports the number of publisher goroutines currently
	// inside a delivery attempt, sampled on every change.
	WorkersActive(ctx context.Context, n int)

	// StatsSampled reports a queue-depth sample: pending, leased and failed
	// counts plus publishing lag.
	StatsSampled(ctx context.Context, s Stats)
}

// PublishOutcome is the result of one delivery attempt.
type PublishOutcome string

// The delivery outcomes.
const (
	// OutcomeSuccess means the publisher returned nil.
	OutcomeSuccess PublishOutcome = "success"
	// OutcomeRetryable means the attempt failed and will be retried.
	OutcomeRetryable PublishOutcome = "retryable"
	// OutcomePermanent means the attempt failed permanently.
	OutcomePermanent PublishOutcome = "permanent"
	// OutcomeAbandoned means the attempt was cut short by shutdown and the
	// message was released for another worker. It is not a failure.
	OutcomeAbandoned PublishOutcome = "abandoned"
)

// PublishEvent describes one delivery attempt.
type PublishEvent struct {
	// Type is the message type. Bounded cardinality is the caller's
	// responsibility.
	Type string
	// Outcome is how the attempt ended.
	Outcome PublishOutcome
	// Attempt is the attempt number, starting at 1.
	Attempt int
	// Duration is the time spent in Publisher.Publish.
	Duration time.Duration
	// Lag is how long the message waited between becoming available and the
	// start of this attempt: the latency operators actually care about.
	Lag time.Duration
}

// NopObserver implements [Observer] with no-op methods. Embed it in your own
// observer so that methods added to the interface in future versions do not
// break your build.
type NopObserver struct{}

// ClaimCompleted does nothing.
func (NopObserver) ClaimCompleted(context.Context, int, int, time.Duration) {}

// PublishCompleted does nothing.
func (NopObserver) PublishCompleted(context.Context, PublishEvent) {}

// RetryScheduled does nothing.
func (NopObserver) RetryScheduled(context.Context, string, int, time.Duration) {}

// MessageFailed does nothing.
func (NopObserver) MessageFailed(context.Context, string, FailureReason, int) {}

// BatchCompleted does nothing.
func (NopObserver) BatchCompleted(context.Context, int, time.Duration) {}

// StoreFailed does nothing.
func (NopObserver) StoreFailed(context.Context, string) {}

// LeaseLost does nothing.
func (NopObserver) LeaseLost(context.Context, string) {}

// WorkersActive does nothing.
func (NopObserver) WorkersActive(context.Context, int) {}

// StatsSampled does nothing.
func (NopObserver) StatsSampled(context.Context, Stats) {}

var _ Observer = NopObserver{}
