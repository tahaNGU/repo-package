package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Worker publishes stored messages in the background. It owns every goroutine
// it starts and nothing else: the store, the publisher and the logger are
// supplied by the caller and are never closed by the worker.
//
// A worker is single-use. [Worker.Start] launches it, [Worker.Shutdown] stops
// it; after shutdown, create a new one.
//
// # Shape
//
// One poller goroutine claims work and hands each message to one of
// Config.Concurrency publisher goroutines. The poller only claims as many
// messages as there are idle publishers, so nothing leased is ever parked in
// memory: at any instant the number of leased messages is at most
// Config.Concurrency, and a slow broker simply stops the worker from claiming
// more. Two further goroutines sample queue depth and, if retention is
// configured, delete published rows.
type Worker struct {
	store Store
	pub   Publisher
	cfg   Config
	log   *slog.Logger

	mu      sync.Mutex
	started bool
	stopped bool

	// ctx is cancelled to abort in-flight publishes; stopPoll is closed to
	// stop claiming new work. Shutdown closes stopPoll, then cancels ctx only
	// if its deadline runs out.
	ctx      context.Context
	cancel   context.CancelFunc
	stopPoll chan struct{}
	stopOnce sync.Once

	wg   sync.WaitGroup
	done chan struct{}

	jobs   chan job
	slots  chan struct{}
	notify chan struct{}

	draining atomic.Bool
	active   atomic.Int64
}

// job is one message handed to a publisher goroutine, together with the batch
// it was claimed in so that batch-level metrics can be reported when the last
// message of the batch finishes.
type job struct {
	msg   Claimed
	batch *batch
}

type batch struct {
	size      int
	start     time.Time
	remaining atomic.Int64
}

// NewWorker returns a worker that publishes messages from store using pub.
// It returns an error only if cfg is invalid or a dependency is nil, so the
// result is always ready to [Worker.Start].
func NewWorker(store Store, pub Publisher, cfg Config) (*Worker, error) {
	if store == nil {
		return nil, errors.New("outbox: store is required")
	}
	if pub == nil {
		return nil, errors.New("outbox: publisher is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	w := &Worker{
		store:    store,
		pub:      pub,
		cfg:      cfg,
		log:      cfg.Logger.With(slog.String("component", "outbox"), slog.String("worker", cfg.Name)),
		stopPoll: make(chan struct{}),
		done:     make(chan struct{}),
		jobs:     make(chan job),
		slots:    make(chan struct{}, cfg.Concurrency),
		notify:   make(chan struct{}, 1),
	}
	for range cfg.Concurrency {
		w.slots <- struct{}{}
	}
	return w, nil
}

// Start launches the worker's goroutines and returns immediately.
//
// The worker stops when ctx is cancelled or when [Worker.Shutdown] is called,
// whichever happens first; ctx is therefore a lifetime, not a deadline, and
// should usually be the application context. Start returns [ErrWorkerRunning]
// if the worker is already running and [ErrWorkerStopped] if it has been shut
// down.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case w.stopped:
		return ErrWorkerStopped
	case w.started:
		return ErrWorkerRunning
	}
	w.started = true
	w.ctx, w.cancel = context.WithCancel(ctx)

	w.wg.Add(1)
	go func() { defer w.wg.Done(); w.poll() }()
	for range w.cfg.Concurrency {
		w.wg.Add(1)
		go func() { defer w.wg.Done(); w.publishLoop() }()
	}
	if w.cfg.StatsInterval > 0 {
		w.wg.Add(1)
		go func() { defer w.wg.Done(); w.sampleStats() }()
	}
	if w.cfg.Retention > 0 {
		if _, ok := w.store.(Pruner); ok {
			w.wg.Add(1)
			go func() { defer w.wg.Done(); w.prune() }()
		} else {
			w.log.Warn("retention configured but store does not implement outbox.Pruner; no messages will be deleted",
				slog.Duration("retention", w.cfg.Retention))
		}
	}

	go func() {
		w.wg.Wait()
		close(w.done)
	}()
	w.log.Info("outbox worker started",
		slog.Int("concurrency", w.cfg.Concurrency),
		slog.Int("batch_size", w.cfg.BatchSize),
		slog.Duration("poll_interval", w.cfg.PollInterval),
		slog.Duration("lease", w.cfg.LeaseDuration),
		slog.Int("max_attempts", w.cfg.MaxAttempts))
	return nil
}

// Shutdown stops the worker within the bounds of ctx.
//
// It stops claiming new work immediately, then waits for in-flight publishes
// to finish. If ctx expires first, the in-flight publish contexts are
// cancelled and Shutdown returns an error wrapping [ErrShutdownTimeout]: those
// messages stay leased until their leases expire and are then retried, by this
// process or another. Messages that were claimed but not yet handed to a
// publisher are released immediately so they are retried without waiting for
// lease expiry.
//
// Shutdown is idempotent and safe to call from multiple goroutines. Calling it
// on a worker that was never started is a no-op. It never marks an unpublished
// message as published.
//
// A publisher that ignores context cancellation can keep running after
// Shutdown returns; the worker does not wait for it, which is the only way to
// keep shutdown bounded.
func (w *Worker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	started := w.started
	w.stopped = true
	w.mu.Unlock()
	if !started {
		return nil
	}

	w.draining.Store(true)
	w.stopOnce.Do(func() { close(w.stopPoll) })

	select {
	case <-w.done:
		w.cancel()
		w.log.Info("outbox worker stopped")
		return nil
	case <-ctx.Done():
		w.cancel() // abort in-flight publishes
		select {
		case <-w.done:
			w.log.Info("outbox worker stopped after cancelling in-flight publishes")
			return nil
		default:
		}
		w.log.Warn("outbox worker shutdown deadline exceeded; in-flight publishes cancelled",
			slog.Int64("active_publishes", w.active.Load()))
		return errors.Join(ErrShutdownTimeout, ctx.Err())
	}
}

// Notify hints that new messages were enqueued, waking the poller instead of
// waiting out the poll interval. It never blocks and may be called from any
// goroutine, including right after a successful commit.
//
// It is an in-process optimisation only: messages enqueued by other processes
// are found by the next poll, so correctness never depends on calling it.
func (w *Worker) Notify() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// poll claims work and hands it to the publisher goroutines.
func (w *Worker) poll() {
	var claimErrs int
	for {
		if w.stopping() {
			return
		}
		// Block until at least one publisher is idle.
		select {
		case <-w.slots:
		case <-w.stopPoll:
			return
		case <-w.ctx.Done():
			return
		}
		held := 1 + w.drainSlots(w.cfg.BatchSize-1)

		start := time.Now()
		res, err := w.claim(held)
		if err != nil {
			w.returnSlots(held)
			if isContextError(err) && w.stopping() {
				return
			}
			claimErrs++
			delay := w.cfg.Backoff.Delay(claimErrs)
			w.log.Error("outbox claim failed",
				slog.String("operation", "claim"),
				slog.Int("consecutive_failures", claimErrs),
				slog.Duration("retry_in", delay),
				slog.Any("error", err))
			w.cfg.Observer.StoreFailed(w.ctx, "claim")
			if !w.sleep(delay) {
				return
			}
			continue
		}
		claimErrs = 0
		w.cfg.Observer.ClaimCompleted(w.ctx, len(res.Messages), res.ExpiredLeases, time.Since(start))
		if res.ExpiredLeases > 0 {
			w.log.Warn("outbox reclaimed expired leases",
				slog.Int("count", res.ExpiredLeases),
				slog.Duration("lease", w.cfg.LeaseDuration))
		}

		dispatched, stopped := w.dispatch(res.Messages)
		w.returnSlots(held - dispatched)
		if stopped {
			return
		}
		if dispatched == 0 {
			if !w.waitForWork() {
				return
			}
			continue
		}
		if dispatched < held {
			// The store had less work than we had capacity for: nothing is
			// waiting, so sleep rather than spin.
			if !w.waitForWork() {
				return
			}
		}
	}
}

// dispatch hands messages to publisher goroutines. It reports how many were
// handed off — the caller owns the slots of the rest — and whether the worker
// stopped mid-batch, in which case the undispatched messages have been
// released so they are retried without waiting for lease expiry.
func (w *Worker) dispatch(msgs []Claimed) (dispatched int, stopped bool) {
	if len(msgs) == 0 {
		return 0, false
	}
	b := &batch{size: len(msgs), start: time.Now()}
	b.remaining.Store(int64(len(msgs)))
	defer func() {
		// Messages that were released instead of dispatched will never reach
		// finishBatch, so settle their share of the batch here.
		if undispatched := int64(len(msgs) - dispatched); undispatched > 0 {
			for range undispatched {
				w.finishBatch(b)
			}
		}
	}()
	for i, m := range msgs {
		select {
		case w.jobs <- job{msg: m, batch: b}:
		case <-w.stopPoll:
			w.releaseAll(msgs[i:])
			return i, true
		case <-w.ctx.Done():
			w.releaseAll(msgs[i:])
			return i, true
		}
	}
	return b.size, false
}

// releaseAll returns claimed-but-undispatched messages to the pending state so
// they are retried immediately instead of after lease expiry.
func (w *Worker) releaseAll(msgs []Claimed) {
	for _, m := range msgs {
		w.release(m)
	}
}

// publishLoop runs one publisher goroutine.
func (w *Worker) publishLoop() {
	for {
		select {
		case j := <-w.jobs:
			w.process(j)
			w.slots <- struct{}{}
		case <-w.ctx.Done():
			return
		case <-w.stopPoll:
			// The poller has stopped claiming. Drain anything already handed
			// off, then exit: once stopPoll is closed the poller releases
			// rather than dispatches, so no further jobs can arrive.
			for {
				select {
				case j := <-w.jobs:
					w.process(j)
					w.slots <- struct{}{}
				default:
					return
				}
			}
		}
	}
}

// process performs one delivery attempt and records its outcome.
func (w *Worker) process(j job) {
	m := j.msg
	batch := j.batch.size
	defer w.finishBatch(j.batch)

	if m.DecodeErr != nil {
		w.log.Error("outbox message could not be decoded; failing permanently",
			msgAttrs(m, batch,
				slog.String("error_kind", string(ReasonUndecodable)),
				slog.Any("error", m.DecodeErr))...)
		w.failMessage(m, batch, ReasonUndecodable, m.DecodeErr)
		return
	}

	w.active.Add(1)
	w.cfg.Observer.WorkersActive(w.ctx, int(w.active.Load()))

	ctx := w.ctx
	var end EndSpan
	if w.cfg.Tracer != nil {
		ctx, end = w.cfg.Tracer.StartPublish(ctx, &m)
	}
	pctx, cancel := context.WithTimeout(ctx, w.cfg.PublishTimeout)
	start := time.Now()
	err := w.publish(pctx, m.Message)
	took := time.Since(start)
	cancel()
	if end != nil {
		end(err)
	}

	w.active.Add(-1)
	w.cfg.Observer.WorkersActive(w.ctx, int(w.active.Load()))

	lag := start.Sub(m.AvailableAt)
	if lag < 0 {
		lag = 0
	}
	event := PublishEvent{Type: m.Type, Attempt: m.Attempt, Duration: took, Lag: lag}

	switch {
	case err == nil:
		event.Outcome = OutcomeSuccess
		w.cfg.Observer.PublishCompleted(w.ctx, event)
		w.markPublished(m, batch, took)

	case w.abandoned(err):
		// Shutdown cut the publish short. The message was not delivered as far
		// as we know, and it was not the message's fault, so put it straight
		// back without recording a failure.
		event.Outcome = OutcomeAbandoned
		w.cfg.Observer.PublishCompleted(w.ctx, event)
		w.log.Info("outbox publish abandoned during shutdown; message released for retry",
			msgAttrs(m, batch)...)
		w.release(m)

	case w.cfg.Classify(err) == PermanentFailure:
		event.Outcome = OutcomePermanent
		w.cfg.Observer.PublishCompleted(w.ctx, event)
		w.log.Error("outbox publish failed permanently",
			msgAttrs(m, batch,
				slog.String("error_kind", string(ReasonPermanent)),
				slog.Duration("duration", took),
				slog.Any("error", err))...)
		w.failMessage(m, batch, ReasonPermanent, err)

	case m.Attempt >= w.cfg.MaxAttempts:
		event.Outcome = OutcomeRetryable
		w.cfg.Observer.PublishCompleted(w.ctx, event)
		w.log.Error("outbox publish failed and retry budget is exhausted",
			msgAttrs(m, batch,
				slog.String("error_kind", string(ReasonExhausted)),
				slog.Int("max_attempts", w.cfg.MaxAttempts),
				slog.Duration("duration", took),
				slog.Any("error", err))...)
		w.failMessage(m, batch, ReasonExhausted, err)

	default:
		event.Outcome = OutcomeRetryable
		w.cfg.Observer.PublishCompleted(w.ctx, event)
		delay := w.cfg.Backoff.Delay(m.Attempt)
		w.log.Warn("outbox publish failed; scheduling retry",
			msgAttrs(m, batch,
				slog.Duration("retry_in", delay),
				slog.Duration("duration", took),
				slog.Any("error", err))...)
		w.reschedule(m, batch, delay, err)
	}
}

// publish calls the publisher, turning a panic into a permanent failure.
//
// A publisher is handed arbitrary stored payloads, so a malformed one can
// panic inside a serialiser or a broker client. Letting that unwind would take
// the whole application down and leak this goroutine's slot, which is a much
// worse outcome than one message in the failed state: the panic is recorded,
// the message is failed permanently so it cannot repeat, and the worker keeps
// running. The stack is logged because it is a bug, not an operational event.
func (w *Worker) publish(ctx context.Context, m Message) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		w.log.Error("outbox publisher panicked; failing the message permanently",
			slog.String("message_id", m.ID.String()),
			slog.String("message_type", m.Type),
			slog.Any("panic", r),
			slog.String("stack", string(debug.Stack())))
		err = Permanent(fmt.Errorf("outbox: publisher panicked: %v", r))
	}()
	return w.pub.Publish(ctx, m)
}

// abandoned reports whether a publication error is the consequence of the
// worker being torn down rather than a failure of the message or the broker.
func (w *Worker) abandoned(err error) bool {
	return isContextError(err) && (w.ctx.Err() != nil || w.draining.Load() && errors.Is(err, context.Canceled))
}

// finishBatch reports batch metrics once every message of the batch has been
// processed.
func (w *Worker) finishBatch(b *batch) {
	if b.remaining.Add(-1) == 0 {
		w.cfg.Observer.BatchCompleted(w.ctx, b.size, time.Since(b.start))
	}
}

// bookkeeping returns a context for a store write that records the outcome of
// a publish. It is deliberately detached from the worker context: once the
// broker has accepted a message, recording that fact is what prevents a
// duplicate, and a cancelled worker context must not stop it. It stays bounded
// by StoreTimeout so shutdown cannot hang.
func (w *Worker) bookkeeping() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(w.ctx), w.cfg.StoreTimeout)
}

func (w *Worker) markPublished(m Claimed, batch int, took time.Duration) {
	ctx, cancel := w.bookkeeping()
	defer cancel()
	err := w.store.MarkPublished(ctx, m.ID, m.LeaseEpoch)
	switch {
	case err == nil:
		// Guarded so that the success path -- the hot one -- allocates
		// nothing for a record that is usually discarded.
		if w.log.Enabled(ctx, slog.LevelDebug) {
			w.log.Debug("outbox message published",
				msgAttrs(m, batch, slog.Duration("duration", took))...)
		}
	case errors.Is(err, ErrLeaseLost):
		// The broker accepted the message but our lease had already been
		// reclaimed, so another worker will publish it again: a duplicate. The
		// fence is what stops us from overwriting the new owner's state.
		w.cfg.Observer.LeaseLost(w.ctx, "publish_ack")
		w.log.Error("outbox lease lost after successful publish; message may be published again",
			msgAttrs(m, batch,
				slog.String("operation", "publish_ack"),
				slog.Int64("lease_epoch", m.LeaseEpoch))...)
	default:
		// Worst case: delivered but not recorded. The lease expires and
		// another attempt produces a duplicate, which is why consumers must be
		// idempotent.
		w.cfg.Observer.StoreFailed(w.ctx, "publish_ack")
		w.log.Error("outbox could not record successful publish; message will be retried and may duplicate",
			msgAttrs(m, batch,
				slog.String("operation", "publish_ack"),
				slog.Any("error", err))...)
	}
}

func (w *Worker) reschedule(m Claimed, batch int, delay time.Duration, cause error) {
	ctx, cancel := w.bookkeeping()
	defer cancel()
	f := Failure{Kind: Retryable.String(), Error: w.cfg.ErrorSummary(cause)}
	if err := w.store.Reschedule(ctx, m.ID, m.LeaseEpoch, delay, f); err != nil {
		w.storeOpFailed("reschedule", err, m, batch)
		return
	}
	w.cfg.Observer.RetryScheduled(w.ctx, m.Type, m.Attempt, delay)
}

func (w *Worker) failMessage(m Claimed, batch int, reason FailureReason, cause error) {
	ctx, cancel := w.bookkeeping()
	defer cancel()
	f := Failure{Kind: string(reason), Error: w.cfg.ErrorSummary(cause)}
	if err := w.store.Fail(ctx, m.ID, m.LeaseEpoch, f); err != nil {
		w.storeOpFailed("fail", err, m, batch)
		return
	}
	w.cfg.Observer.MessageFailed(w.ctx, m.Type, reason, m.Attempt)
}

func (w *Worker) release(m Claimed) {
	ctx, cancel := w.bookkeeping()
	defer cancel()
	if err := w.store.Release(ctx, m.ID, m.LeaseEpoch); err != nil {
		w.storeOpFailed("release", err, m, 0)
	}
}

// storeOpFailed reports a failed bookkeeping write. A lost lease is expected
// under lease expiry and is reported separately from a broken database.
func (w *Worker) storeOpFailed(op string, err error, m Claimed, batch int) {
	if errors.Is(err, ErrLeaseLost) {
		w.cfg.Observer.LeaseLost(w.ctx, op)
		w.log.Warn("outbox update rejected: lease no longer held",
			msgAttrs(m, batch, slog.String("operation", op))...)
		return
	}
	w.cfg.Observer.StoreFailed(w.ctx, op)
	w.log.Error("outbox store operation failed",
		msgAttrs(m, batch, slog.String("operation", op), slog.Any("error", err))...)
}

// msgAttrs builds the standard log attributes for a message, plus any extras.
// It is called where a record is actually emitted rather than once per
// message, because formatting an id and allocating an attribute slice for a
// log line that is never written is pure waste on the publishing path.
func msgAttrs(m Claimed, batch int, extra ...any) []any {
	attrs := make([]any, 0, 4+len(extra))
	attrs = append(attrs,
		slog.String("message_id", m.ID.String()),
		slog.String("message_type", m.Type),
		slog.Int("attempt", m.Attempt))
	if batch > 0 {
		attrs = append(attrs, slog.Int("batch_size", batch))
	}
	return append(attrs, extra...)
}

func (w *Worker) claim(limit int) (ClaimResult, error) {
	ctx, cancel := context.WithTimeout(w.ctx, w.cfg.StoreTimeout)
	defer cancel()
	return w.store.Claim(ctx, ClaimRequest{
		Owner: w.cfg.Name,
		Limit: limit,
		Lease: w.cfg.LeaseDuration,
	})
}

// sampleStats periodically reports queue depth and lag.
func (w *Worker) sampleStats() {
	t := time.NewTicker(w.cfg.StatsInterval)
	defer t.Stop()
	for {
		w.observeStats()
		select {
		case <-t.C:
		case <-w.stopPoll:
			return
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Worker) observeStats() {
	ctx, cancel := context.WithTimeout(w.ctx, w.cfg.StoreTimeout)
	defer cancel()
	s, err := w.store.Stats(ctx)
	if err != nil {
		if isContextError(err) && w.stopping() {
			return
		}
		w.cfg.Observer.StoreFailed(w.ctx, "stats")
		w.log.Error("outbox stats sampling failed",
			slog.String("operation", "stats"), slog.Any("error", err))
		return
	}
	w.cfg.Observer.StatsSampled(w.ctx, s)
	w.log.Debug("outbox stats",
		slog.Int64("pending", s.Pending),
		slog.Int64("leased", s.Leased),
		slog.Int64("failed", s.Failed),
		slog.Duration("oldest_pending_age", s.OldestPendingAge))
}

// prune deletes published messages older than the retention window.
func (w *Worker) prune() {
	pruner := w.store.(Pruner)
	t := time.NewTicker(w.cfg.PruneInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-w.stopPoll:
			return
		case <-w.ctx.Done():
			return
		}
		for {
			ctx, cancel := context.WithTimeout(w.ctx, w.cfg.StoreTimeout)
			n, err := pruner.DeletePublished(ctx, w.cfg.Retention, w.cfg.PruneBatch)
			cancel()
			if err != nil {
				if isContextError(err) && w.stopping() {
					return
				}
				w.cfg.Observer.StoreFailed(w.ctx, "prune")
				w.log.Error("outbox retention sweep failed",
					slog.String("operation", "prune"), slog.Any("error", err))
				break
			}
			if n > 0 {
				w.log.Info("outbox retention sweep deleted published messages",
					slog.Int("deleted", n), slog.Duration("retention", w.cfg.Retention))
			}
			if n < w.cfg.PruneBatch {
				break
			}
			if w.stopping() {
				return
			}
		}
	}
}

// waitForWork sleeps until the poll interval elapses, a Notify arrives, or the
// worker stops. It reports false when the worker should stop.
func (w *Worker) waitForWork() bool {
	// A little jitter keeps replicas from polling in lockstep.
	d := w.cfg.PollInterval
	if d > 0 {
		d += time.Duration(rand.Int64N(int64(d/4) + 1))
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-w.notify:
		return true
	case <-w.stopPoll:
		return false
	case <-w.ctx.Done():
		return false
	}
}

func (w *Worker) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-w.stopPoll:
		return false
	case <-w.ctx.Done():
		return false
	}
}

func (w *Worker) stopping() bool {
	select {
	case <-w.stopPoll:
		return true
	case <-w.ctx.Done():
		return true
	default:
		return false
	}
}

// drainSlots takes up to n further idle publisher slots without blocking.
func (w *Worker) drainSlots(n int) int {
	var got int
	for got < n {
		select {
		case <-w.slots:
			got++
		default:
			return got
		}
	}
	return got
}

func (w *Worker) returnSlots(n int) {
	for range n {
		w.slots <- struct{}{}
	}
}
