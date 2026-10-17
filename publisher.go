package outbox

import "context"

// Publisher delivers a message to the outside world: a broker, a queue, an
// HTTP endpoint. It is the only part of the pipeline the application has to
// write.
//
// Implementations must:
//
//   - Honour ctx. The worker bounds every call with Config.PublishTimeout and
//     cancels it on shutdown; a publisher that ignores ctx can delay shutdown
//     past its deadline and can outlive its lease.
//   - Be safe for concurrent use. The worker calls Publish from up to
//     Config.Concurrency goroutines.
//   - Return a [Permanent]-wrapped error for failures retrying cannot fix. Any
//     other error is retried with backoff.
//   - Treat delivery as at-least-once: the same message may be published more
//     than once (see the package documentation), so consumers must be
//     idempotent. Propagating m.ID to the broker (as a message id or a
//     deduplication key) is what makes that possible.
type Publisher interface {
	Publish(ctx context.Context, m Message) error
}

// PublisherFunc adapts a function to [Publisher].
type PublisherFunc func(ctx context.Context, m Message) error

// Publish calls f.
func (f PublisherFunc) Publish(ctx context.Context, m Message) error { return f(ctx, m) }
