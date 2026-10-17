package outbox

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Default worker settings. They are chosen to be safe on an ordinary
// deployment: a few concurrent publishes, a one-second poll, and a lease long
// enough to cover a slow publish plus its bookkeeping.
const (
	DefaultConcurrency    = 8
	DefaultBatchSize      = 64
	DefaultPollInterval   = time.Second
	DefaultLeaseDuration  = 30 * time.Second
	DefaultPublishTimeout = 10 * time.Second
	DefaultStoreTimeout   = 5 * time.Second
	DefaultMaxAttempts    = 10
	DefaultStatsInterval  = 15 * time.Second
	DefaultPruneInterval  = 10 * time.Minute
	DefaultPruneBatch     = 1000
)

// Config configures a [Worker]. The zero value is valid: every field has a
// documented default, so `outbox.Config{}` is a reasonable starting point.
//
// Three settings are related and are checked against each other:
//
//	PublishTimeout + StoreTimeout <= LeaseDuration
//
// A worker holds a lease for at most one publish plus the store write that
// records its outcome. If the lease were shorter, another worker could reclaim
// a message that is still being published, which turns into a duplicate. The
// worker claims at most as many messages as it has idle publisher goroutines,
// so BatchSize does not enter the inequality: claimed work is never parked in
// memory waiting for a free goroutine.
type Config struct {
	// Concurrency is the number of goroutines publishing in parallel, and
	// therefore the maximum number of simultaneously leased messages.
	// Defaults to DefaultConcurrency. It should be no larger than the share of
	// the database connection pool this worker may use.
	Concurrency int

	// BatchSize caps how many messages a single claim query leases. The
	// effective batch is min(BatchSize, idle goroutines), so values above
	// Concurrency have no effect. Defaults to DefaultBatchSize.
	BatchSize int

	// PollInterval is how long the poller sleeps after finding no work. After
	// a full batch it polls again immediately, so a backlog drains at full
	// speed. Defaults to DefaultPollInterval.
	PollInterval time.Duration

	// LeaseDuration is how long a claim is valid. It also bounds crash
	// recovery: after a worker dies, its messages are retried once their
	// leases expire. Defaults to DefaultLeaseDuration.
	LeaseDuration time.Duration

	// PublishTimeout bounds a single Publisher.Publish call. Defaults to
	// DefaultPublishTimeout.
	PublishTimeout time.Duration

	// StoreTimeout bounds every individual store operation. Defaults to
	// DefaultStoreTimeout.
	StoreTimeout time.Duration

	// MaxAttempts is the delivery budget per message. Once a message has
	// started this many attempts without success it moves to the failed state.
	// Defaults to DefaultMaxAttempts. Attempts are counted when a message is
	// claimed, so a message whose worker crashes mid-publish still consumes
	// one; that is what stops a poisonous payload from killing workers
	// forever.
	MaxAttempts int

	// Backoff is the retry schedule. The zero value means DefaultBackoff.
	Backoff Backoff

	// Classify decides whether a publication error is retryable. Defaults to
	// [Classify], which treats errors wrapped with [Permanent] as permanent
	// and everything else as retryable.
	Classify func(error) Disposition

	// ErrorSummary renders a failure for storage in the outbox row. The
	// default returns the error chain truncated to 512 bytes. Override it to
	// redact errors that may embed credentials or personal data.
	ErrorSummary func(error) string

	// StatsInterval is how often queue depth and lag are sampled and reported
	// to the Observer. Each sample runs index scans proportional to the number
	// of unpublished and failed rows, so it is deliberately infrequent.
	// Defaults to DefaultStatsInterval; a negative value disables sampling.
	StatsInterval time.Duration

	// Retention enables deleting published messages older than this duration.
	// Zero (the default) disables deletion: nothing is ever removed from the
	// table and retention is left to operators. Only messages in the published
	// state are ever deleted. Requires a store that implements [Pruner].
	Retention time.Duration

	// PruneInterval is how often the retention sweep runs when Retention is
	// set. Defaults to DefaultPruneInterval.
	PruneInterval time.Duration

	// PruneBatch caps how many rows one retention sweep deletes, keeping the
	// delete transaction short. Defaults to DefaultPruneBatch.
	PruneBatch int

	// Name identifies this worker in logs and in the lease_owner column. It
	// defaults to "hostname/pid". Give replicas distinct names if the hostname
	// is not already unique.
	Name string

	// Logger receives structured logs. Defaults to [slog.Default]. Normal
	// operation logs at debug level; retryable publication failures log at
	// warn; terminal failures, lost leases and store errors log at error.
	Logger *slog.Logger

	// Observer receives metrics signals. Defaults to [NopObserver].
	Observer Observer

	// Tracer creates one span per delivery attempt. Optional.
	Tracer Tracer
}

func (c Config) withDefaults() Config {
	if c.Concurrency <= 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.PollInterval == 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = DefaultLeaseDuration
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = DefaultPublishTimeout
	}
	if c.StoreTimeout == 0 {
		c.StoreTimeout = DefaultStoreTimeout
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = DefaultMaxAttempts
	}
	if c.StatsInterval == 0 {
		c.StatsInterval = DefaultStatsInterval
	}
	if c.PruneInterval == 0 {
		c.PruneInterval = DefaultPruneInterval
	}
	if c.PruneBatch == 0 {
		c.PruneBatch = DefaultPruneBatch
	}
	c.Backoff = c.Backoff.withDefaults()
	if c.Classify == nil {
		c.Classify = Classify
	}
	if c.ErrorSummary == nil {
		c.ErrorSummary = defaultErrorSummary
	}
	if c.Name == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		c.Name = host + "/" + strconv.Itoa(os.Getpid())
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Observer == nil {
		c.Observer = NopObserver{}
	}
	return c
}

// Validate reports whether the configuration is usable. [NewWorker] calls it,
// so applications rarely need to.
func (c Config) Validate() error {
	var errs []error
	bad := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("%w: "+format, append([]any{ErrInvalidConfig}, args...)...))
	}
	if c.Concurrency < 0 {
		bad("concurrency must not be negative")
	}
	if c.BatchSize < 0 {
		bad("batch size must not be negative")
	}
	if c.PollInterval < 0 {
		bad("poll interval must not be negative")
	}
	if c.LeaseDuration < 0 {
		bad("lease duration must not be negative")
	}
	if c.PublishTimeout < 0 {
		bad("publish timeout must not be negative")
	}
	if c.StoreTimeout < 0 {
		bad("store timeout must not be negative")
	}
	if c.MaxAttempts < 0 {
		bad("max attempts must not be negative")
	}
	if c.Retention < 0 {
		bad("retention must not be negative")
	}
	if c.PruneBatch < 0 {
		bad("prune batch must not be negative")
	}
	if c.PruneInterval < 0 {
		bad("prune interval must not be negative")
	}
	if err := c.Backoff.validate(); err != nil {
		errs = append(errs, err)
	}

	d := c.withDefaults()
	if d.PublishTimeout+d.StoreTimeout > d.LeaseDuration {
		bad("lease duration %s is shorter than publish timeout %s plus store timeout %s: "+
			"a lease could expire while a publish is still running",
			d.LeaseDuration, d.PublishTimeout, d.StoreTimeout)
	}
	return errors.Join(errs...)
}
