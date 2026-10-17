package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tahaNGU/gocast"
	"github.com/tahaNGU/gocast/memstore"
)

// ---------- helpers ----------

func testConfig() outbox.Config {
	return outbox.Config{
		Concurrency:    2,
		BatchSize:      8,
		PollInterval:   2 * time.Millisecond,
		LeaseDuration:  2 * time.Second,
		PublishTimeout: 500 * time.Millisecond,
		StoreTimeout:   500 * time.Millisecond,
		MaxAttempts:    3,
		Backoff:        outbox.Backoff{Initial: 5 * time.Millisecond, Max: 20 * time.Millisecond, Multiplier: 2},
		StatsInterval:  -1, // sampling is exercised by its own test
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func enqueue(t *testing.T, s *memstore.Store, msgs ...outbox.Message) []outbox.ID {
	t.Helper()
	for i := range msgs {
		if msgs[i].ID.IsZero() {
			msgs[i].ID = outbox.NewID()
		}
		if msgs[i].Type == "" {
			msgs[i].Type = "test.event.v1"
		}
	}
	tx := s.Begin()
	if err := s.Enqueue(context.Background(), tx, msgs...); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	ids := make([]outbox.ID, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids
}

// startWorker starts a worker and guarantees it is shut down when the test ends.
func startWorker(t *testing.T, store outbox.Store, pub outbox.Publisher, cfg outbox.Config) *outbox.Worker {
	t.Helper()
	w, err := outbox.NewWorker(store, pub, cfg)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.Shutdown(ctx)
	})
	return w
}

// recorder is a Publisher that records what it was asked to publish.
type recorder struct {
	mu       sync.Mutex
	messages []outbox.Message
	fn       func(attempt int, m outbox.Message) error
	calls    atomic.Int64
}

func (r *recorder) Publish(ctx context.Context, m outbox.Message) error {
	n := int(r.calls.Add(1))
	r.mu.Lock()
	r.messages = append(r.messages, m)
	r.mu.Unlock()
	if r.fn != nil {
		return r.fn(n, m)
	}
	return nil
}

func (r *recorder) published() []outbox.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]outbox.Message(nil), r.messages...)
}

// observer counts the signals the worker emits.
type observer struct {
	outbox.NopObserver
	mu          sync.Mutex
	claims      int
	claimed     int
	expired     int
	publishes   map[outbox.PublishOutcome]int
	retries     int
	failures    map[outbox.FailureReason]int
	storeFails  map[string]int
	leaseLost   map[string]int
	maxActive   int
	batches     []int
	lastStats   outbox.Stats
	statSamples int
}

func newObserver() *observer {
	return &observer{
		publishes:  map[outbox.PublishOutcome]int{},
		failures:   map[outbox.FailureReason]int{},
		storeFails: map[string]int{},
		leaseLost:  map[string]int{},
	}
}

func (o *observer) ClaimCompleted(_ context.Context, claimed, expired int, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claims++
	o.claimed += claimed
	o.expired += expired
}
func (o *observer) PublishCompleted(_ context.Context, e outbox.PublishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.publishes[e.Outcome]++
}
func (o *observer) RetryScheduled(_ context.Context, _ string, _ int, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retries++
}
func (o *observer) MessageFailed(_ context.Context, _ string, r outbox.FailureReason, _ int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures[r]++
}
func (o *observer) BatchCompleted(_ context.Context, size int, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.batches = append(o.batches, size)
}
func (o *observer) StoreFailed(_ context.Context, op string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.storeFails[op]++
}
func (o *observer) LeaseLost(_ context.Context, op string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.leaseLost[op]++
}
func (o *observer) WorkersActive(_ context.Context, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.maxActive = max(o.maxActive, n)
}
func (o *observer) StatsSampled(_ context.Context, s outbox.Stats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.statSamples++
	o.lastStats = s
}
func (o *observer) snapshot(fn func(*observer)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fn(o)
}

// ---------- construction ----------

func TestNewWorkerValidatesDependencies(t *testing.T) {
	pub := outbox.PublisherFunc(func(context.Context, outbox.Message) error { return nil })
	if _, err := outbox.NewWorker(nil, pub, outbox.Config{}); err == nil {
		t.Error("accepted a nil store")
	}
	if _, err := outbox.NewWorker(memstore.New(), nil, outbox.Config{}); err == nil {
		t.Error("accepted a nil publisher")
	}
	_, err := outbox.NewWorker(memstore.New(), pub, outbox.Config{LeaseDuration: time.Millisecond, PublishTimeout: time.Minute})
	if !errors.Is(err, outbox.ErrInvalidConfig) {
		t.Errorf("invalid config error = %v, want ErrInvalidConfig", err)
	}
}

// ---------- happy path ----------

func TestWorkerPublishesEnqueuedMessages(t *testing.T) {
	store := memstore.New()
	rec := &recorder{}
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs

	ids := enqueue(t, store,
		outbox.Message{Type: "orders.created.v1", Payload: []byte(`{"id":1}`), ContentType: "application/json"},
		outbox.Message{Type: "orders.created.v1", Payload: []byte(`{"id":2}`)},
	)
	startWorker(t, store, rec, cfg)

	waitFor(t, "both messages published", func() bool {
		return store.CountState(outbox.StatePublished) == 2
	})
	for _, id := range ids {
		snap, _ := store.Get(id)
		if snap.State != outbox.StatePublished {
			t.Errorf("%s: state %s, want published", id, snap.State)
		}
		if snap.Attempts != 1 {
			t.Errorf("%s: attempts %d, want 1", id, snap.Attempts)
		}
		if snap.PublishedAt.IsZero() {
			t.Errorf("%s: published_at not recorded", id)
		}
	}
	got := rec.published()
	if len(got) != 2 {
		t.Fatalf("publisher saw %d messages, want 2", len(got))
	}
	if string(got[0].Payload) == "" || got[0].ID.IsZero() {
		t.Error("publisher received an incomplete message")
	}
	obs.snapshot(func(o *observer) {
		if o.publishes[outbox.OutcomeSuccess] != 2 {
			t.Errorf("success count = %d, want 2", o.publishes[outbox.OutcomeSuccess])
		}
		if o.claimed != 2 {
			t.Errorf("claimed count = %d, want 2", o.claimed)
		}
		if len(o.batches) == 0 {
			t.Error("no batch metrics reported")
		}
		if o.maxActive == 0 {
			t.Error("active publisher gauge never moved")
		}
	})
}

func TestWorkerPreservesMessageFields(t *testing.T) {
	store := memstore.New()
	var got outbox.Message
	var once sync.Once
	done := make(chan struct{})
	rec := outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		once.Do(func() { got = m; close(done) })
		return nil
	})
	id := outbox.NewID()
	enqueue(t, store, outbox.Message{
		ID:            id,
		Type:          "orders.shipped.v2",
		Payload:       []byte("payload"),
		ContentType:   "application/json",
		AggregateType: "order",
		AggregateID:   "order-42",
		OrderingKey:   "order-42",
		Headers:       map[string]string{"tenant": "acme", outbox.HeaderTraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	})
	startWorker(t, store, rec, testConfig())

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("message was never published")
	}
	if got.ID != id || got.Type != "orders.shipped.v2" || got.AggregateID != "order-42" ||
		got.ContentType != "application/json" || got.OrderingKey != "order-42" ||
		got.Headers["tenant"] != "acme" {
		t.Fatalf("publisher received %+v, fields were lost", got)
	}
	if _, _, _, ok := outbox.TraceParent(got.Headers); !ok {
		t.Error("trace context did not survive the round trip")
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt was not populated by the store")
	}
}

// ---------- retry ----------

func TestWorkerRetriesRetryableFailures(t *testing.T) {
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	rec := &recorder{fn: func(attempt int, _ outbox.Message) error {
		if attempt < 3 {
			return errors.New("broker unavailable")
		}
		return nil
	}}
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, rec, cfg)

	waitFor(t, "message published after retries", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StatePublished
	})
	snap, _ := store.Get(ids[0])
	if snap.Attempts != 3 {
		t.Errorf("attempts = %d, want 3", snap.Attempts)
	}
	obs.snapshot(func(o *observer) {
		if o.retries != 2 {
			t.Errorf("retries scheduled = %d, want 2", o.retries)
		}
		if o.publishes[outbox.OutcomeRetryable] != 2 {
			t.Errorf("retryable outcomes = %d, want 2", o.publishes[outbox.OutcomeRetryable])
		}
	})
}

func TestWorkerPersistsRetryScheduleAcrossRestart(t *testing.T) {
	store := memstore.New()
	// A long backoff means the retry cannot happen during this test unless the
	// schedule is forgotten, which is exactly what we are checking.
	cfg := testConfig()
	cfg.Backoff = outbox.Backoff{Initial: time.Hour, Max: time.Hour, Multiplier: 1}
	rec := &recorder{fn: func(int, outbox.Message) error { return errors.New("down") }}
	ids := enqueue(t, store, outbox.Message{Type: "t"})

	w, err := outbox.NewWorker(store, rec, cfg)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, "first attempt to fail", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.Attempts == 1 && snap.State == outbox.StatePending
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	before, _ := store.Get(ids[0])
	if before.AvailableAt.Before(time.Now().Add(30 * time.Minute)) {
		t.Fatalf("available_at = %s, want ~1h in the future", before.AvailableAt)
	}

	// A fresh worker (a restarted process) must honour the stored schedule.
	startWorker(t, store, rec, cfg)
	time.Sleep(100 * time.Millisecond)
	after, _ := store.Get(ids[0])
	if after.Attempts != 1 {
		t.Fatalf("attempts = %d after restart, want the stored schedule to hold at 1", after.Attempts)
	}
	if !after.AvailableAt.Equal(before.AvailableAt) {
		t.Fatalf("available_at changed across restart: %s -> %s", before.AvailableAt, after.AvailableAt)
	}
}

func TestWorkerFailsPermanentlyWithoutRetrying(t *testing.T) {
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	rec := &recorder{fn: func(int, outbox.Message) error {
		return outbox.Permanent(errors.New("schema rejected"))
	}}
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, rec, cfg)

	waitFor(t, "permanent failure", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StateFailed
	})
	snap, _ := store.Get(ids[0])
	if snap.Attempts != 1 {
		t.Errorf("attempts = %d, want 1: a permanent error must not consume the budget", snap.Attempts)
	}
	if snap.ErrorKind != string(outbox.ReasonPermanent) {
		t.Errorf("error kind = %q, want %q", snap.ErrorKind, outbox.ReasonPermanent)
	}
	if snap.LastError == "" {
		t.Error("no error summary stored")
	}
	time.Sleep(50 * time.Millisecond)
	if n := rec.calls.Load(); n != 1 {
		t.Errorf("publisher called %d times, want exactly 1", n)
	}
	obs.snapshot(func(o *observer) {
		if o.failures[outbox.ReasonPermanent] != 1 {
			t.Errorf("permanent failures = %d, want 1", o.failures[outbox.ReasonPermanent])
		}
	})
}

func TestWorkerFailsAfterExhaustingAttempts(t *testing.T) {
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	cfg.MaxAttempts = 3
	rec := &recorder{fn: func(int, outbox.Message) error { return errors.New("still down") }}
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, rec, cfg)

	waitFor(t, "retry budget exhaustion", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StateFailed
	})
	snap, _ := store.Get(ids[0])
	if snap.Attempts != 3 {
		t.Errorf("attempts = %d, want 3", snap.Attempts)
	}
	if snap.ErrorKind != string(outbox.ReasonExhausted) {
		t.Errorf("error kind = %q, want %q", snap.ErrorKind, outbox.ReasonExhausted)
	}
	time.Sleep(50 * time.Millisecond)
	if n := rec.calls.Load(); n != 3 {
		t.Errorf("publisher called %d times, want exactly MaxAttempts=3", n)
	}
	obs.snapshot(func(o *observer) {
		if o.failures[outbox.ReasonExhausted] != 1 {
			t.Errorf("exhausted failures = %d, want 1", o.failures[outbox.ReasonExhausted])
		}
	})

	// A failed message is inspectable and recoverable.
	failed, err := store.ListFailed(context.Background(), 10, outbox.ID{})
	if err != nil {
		t.Fatalf("ListFailed: %v", err)
	}
	if len(failed) != 1 || failed[0].ID != ids[0] {
		t.Fatalf("ListFailed = %+v, want the failed message", failed)
	}
	rec.fn = nil // broker recovered
	n, err := store.Requeue(context.Background(), ids[0])
	if err != nil || n != 1 {
		t.Fatalf("Requeue = %d, %v", n, err)
	}
	waitFor(t, "requeued message to publish", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StatePublished
	})
}

func TestWorkerFailsUndecodableMessageWithoutPublishing(t *testing.T) {
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	rec := &recorder{}
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	store.Corrupt(ids[0])
	startWorker(t, store, rec, cfg)

	waitFor(t, "undecodable message to fail", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StateFailed
	})
	if n := rec.calls.Load(); n != 0 {
		t.Errorf("publisher was called %d times for an undecodable message", n)
	}
	snap, _ := store.Get(ids[0])
	if snap.ErrorKind != string(outbox.ReasonUndecodable) {
		t.Errorf("error kind = %q, want %q", snap.ErrorKind, outbox.ReasonUndecodable)
	}
	obs.snapshot(func(o *observer) {
		if o.failures[outbox.ReasonUndecodable] != 1 {
			t.Errorf("undecodable failures = %d, want 1", o.failures[outbox.ReasonUndecodable])
		}
		if len(o.batches) == 0 {
			t.Error("batch metrics must still be reported for undecodable rows")
		}
	})
}

// ---------- transaction integration ----------

func TestRollbackLeavesNoOutboxRows(t *testing.T) {
	store := memstore.New()
	tx := store.Begin()
	if err := store.Enqueue(context.Background(), tx, outbox.Message{ID: outbox.NewID(), Type: "t"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if store.Len() != 0 {
		t.Fatal("an uncommitted message is already visible")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if store.Len() != 0 {
		t.Fatal("rollback left an outbox row behind")
	}
	if err := store.Enqueue(context.Background(), tx, outbox.Message{ID: outbox.NewID(), Type: "t"}); err == nil {
		t.Fatal("enqueue on a closed transaction must fail")
	}
}

func TestEnqueueValidatesAndAssignsIDs(t *testing.T) {
	store := memstore.New()
	if err := store.Enqueue(context.Background(), nil, outbox.Message{}); !errors.Is(err, outbox.ErrInvalidMessage) {
		t.Fatalf("enqueue of an invalid message = %v, want ErrInvalidMessage", err)
	}
	if store.Len() != 0 {
		t.Fatal("an invalid batch was partially stored")
	}
	if err := store.Enqueue(context.Background(), nil, outbox.Message{Type: "t"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	all := store.All()
	if len(all) != 1 || all[0].ID.IsZero() {
		t.Fatalf("store did not assign an id: %+v", all)
	}
}

// ---------- shutdown ----------

func TestShutdownWaitsForInFlightPublish(t *testing.T) {
	store := memstore.New()
	release := make(chan struct{})
	entered := make(chan struct{})
	pub := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	w, _ := outbox.NewWorker(store, pub, testConfig())
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-entered

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownDone <- w.Shutdown(ctx)
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned %v while a publish was in flight", err)
	default:
	}
	close(release) // publish succeeds

	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	snap, _ := store.Get(ids[0])
	if snap.State != outbox.StatePublished {
		t.Fatalf("state = %s, want published: a graceful shutdown must record the result", snap.State)
	}
}

func TestShutdownIsBoundedAndReleasesAbandonedMessages(t *testing.T) {
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	blocked := make(chan struct{})
	var once sync.Once
	pub := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		once.Do(func() { close(blocked) })
		<-ctx.Done() // honours cancellation, but only when asked
		return ctx.Err()
	})
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	w, _ := outbox.NewWorker(store, pub, cfg)
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-blocked

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := w.Shutdown(ctx)
	if !errors.Is(err, outbox.ErrShutdownTimeout) {
		t.Fatalf("Shutdown error = %v, want ErrShutdownTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Shutdown took %s, far beyond its deadline", elapsed)
	}
	waitFor(t, "abandoned message to be released", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StatePending
	})
	snap, _ := store.Get(ids[0])
	if snap.State == outbox.StatePublished {
		t.Fatal("an abandoned publish must never be recorded as published")
	}
	obs.snapshot(func(o *observer) {
		if o.publishes[outbox.OutcomeAbandoned] != 1 {
			t.Errorf("abandoned outcomes = %d, want 1", o.publishes[outbox.OutcomeAbandoned])
		}
	})
}

func TestShutdownIsIdempotentAndConcurrencySafe(t *testing.T) {
	store := memstore.New()
	w, _ := outbox.NewWorker(store, &recorder{}, testConfig())

	// Shutdown before Start is a no-op.
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Start = %v, want nil", err)
	}
	if err := w.Start(context.Background()); !errors.Is(err, outbox.ErrWorkerStopped) {
		t.Fatalf("Start after Shutdown = %v, want ErrWorkerStopped", err)
	}

	w2, _ := outbox.NewWorker(store, &recorder{}, testConfig())
	if err := w2.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := w2.Start(context.Background()); !errors.Is(err, outbox.ErrWorkerRunning) {
		t.Fatalf("second Start = %v, want ErrWorkerRunning", err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errs[i] = w2.Shutdown(ctx)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent Shutdown %d = %v, want nil", i, err)
		}
	}
}

func TestWorkerStopsWhenStartContextIsCancelled(t *testing.T) {
	store := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	w, _ := outbox.NewWorker(store, &recorder{}, testConfig())
	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	enqueue(t, store, outbox.Message{Type: "t"})
	waitFor(t, "first message published", func() bool { return store.CountState(outbox.StatePublished) == 1 })

	cancel()
	// After the lifetime context is cancelled the worker must stop claiming.
	time.Sleep(50 * time.Millisecond)
	enqueue(t, store, outbox.Message{Type: "t"})
	time.Sleep(100 * time.Millisecond)
	if n := store.CountState(outbox.StatePublished); n != 1 {
		t.Fatalf("published %d messages after cancellation, want 1", n)
	}
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown after cancellation = %v", err)
	}
}

func TestWorkerLeaksNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	store := memstore.New()
	cfg := testConfig()
	cfg.Concurrency = 6
	cfg.StatsInterval = 10 * time.Millisecond
	cfg.Retention = time.Hour
	cfg.PruneInterval = 10 * time.Millisecond

	w, _ := outbox.NewWorker(store, &recorder{}, cfg)
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	enqueue(t, store, outbox.Message{Type: "t"}, outbox.Message{Type: "t"})
	waitFor(t, "messages published", func() bool { return store.CountState(outbox.StatePublished) == 2 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines grew from %d to %d after shutdown", before, runtime.NumGoroutine())
}

// ---------- concurrency and fencing ----------

func TestWorkerRespectsConcurrencyLimit(t *testing.T) {
	store := memstore.New()
	cfg := testConfig()
	cfg.Concurrency = 3
	cfg.BatchSize = 50

	var active, peak atomic.Int64
	pub := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		return nil
	})
	msgs := make([]outbox.Message, 60)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t", Payload: []byte(fmt.Sprint(i))}
	}
	enqueue(t, store, msgs...)
	startWorker(t, store, pub, cfg)

	waitFor(t, "all messages published", func() bool {
		return store.CountState(outbox.StatePublished) == len(msgs)
	})
	if p := peak.Load(); p > 3 {
		t.Fatalf("peak concurrent publishes = %d, want <= 3", p)
	}
	if p := peak.Load(); p < 2 {
		t.Fatalf("peak concurrent publishes = %d, concurrency is not being used", p)
	}
}

func TestStaleWorkerCannotOverwriteReclaimedMessage(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()
	enqueue(t, store, outbox.Message{Type: "t"})

	first, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "worker-a", Limit: 1, Lease: time.Millisecond})
	if err != nil || len(first.Messages) != 1 {
		t.Fatalf("first claim = %+v, %v", first, err)
	}
	time.Sleep(5 * time.Millisecond) // the lease expires

	second, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "worker-b", Limit: 1, Lease: time.Minute})
	if err != nil || len(second.Messages) != 1 {
		t.Fatalf("reclaim = %+v, %v", second, err)
	}
	if second.ExpiredLeases != 1 {
		t.Errorf("expired leases reported = %d, want 1", second.ExpiredLeases)
	}
	if second.Messages[0].LeaseEpoch <= first.Messages[0].LeaseEpoch {
		t.Fatal("reclaiming must advance the lease epoch")
	}
	if !second.Messages[0].ReclaimedLease {
		t.Error("reclaimed message not flagged")
	}
	if second.Messages[0].Attempt != 2 {
		t.Errorf("attempt = %d, want 2", second.Messages[0].Attempt)
	}

	stale := first.Messages[0]
	for name, err := range map[string]error{
		"mark_published": store.MarkPublished(ctx, stale.ID, stale.LeaseEpoch),
		"reschedule":     store.Reschedule(ctx, stale.ID, stale.LeaseEpoch, time.Second, outbox.Failure{}),
		"fail":           store.Fail(ctx, stale.ID, stale.LeaseEpoch, outbox.Failure{}),
		"release":        store.Release(ctx, stale.ID, stale.LeaseEpoch),
	} {
		if !errors.Is(err, outbox.ErrLeaseLost) {
			t.Errorf("%s with a stale lease = %v, want ErrLeaseLost", name, err)
		}
	}
	snap, _ := store.Get(stale.ID)
	if snap.State != outbox.StateLeased {
		t.Fatalf("state = %s, want the new owner's lease to survive", snap.State)
	}
}

func TestConcurrentClaimsNeverOverlap(t *testing.T) {
	store := memstore.New()
	const total = 200
	msgs := make([]outbox.Message, total)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t"}
	}
	enqueue(t, store, msgs...)

	var mu sync.Mutex
	seen := make(map[outbox.ID]string)
	var wg sync.WaitGroup
	for w := range 8 {
		owner := fmt.Sprintf("worker-%d", w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				res, err := store.Claim(context.Background(), outbox.ClaimRequest{
					Owner: owner, Limit: 7, Lease: time.Minute,
				})
				if err != nil {
					t.Error(err)
					return
				}
				if len(res.Messages) == 0 {
					return
				}
				mu.Lock()
				for _, m := range res.Messages {
					if prev, dup := seen[m.ID]; dup {
						t.Errorf("%s claimed by both %s and %s", m.ID, prev, owner)
					}
					seen[m.ID] = owner
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("claimed %d distinct messages, want %d", len(seen), total)
	}
}

func TestMultipleWorkersShareTheBacklogWithoutDuplicating(t *testing.T) {
	store := memstore.New()
	const total = 300
	msgs := make([]outbox.Message, total)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t", Payload: []byte(fmt.Sprint(i))}
	}
	ids := enqueue(t, store, msgs...)

	var mu sync.Mutex
	delivered := make(map[outbox.ID]int)
	pub := outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		mu.Lock()
		delivered[m.ID]++
		mu.Unlock()
		return nil
	})
	for range 4 {
		cfg := testConfig()
		cfg.Concurrency = 4
		startWorker(t, store, pub, cfg)
	}
	waitFor(t, "all messages published", func() bool {
		return store.CountState(outbox.StatePublished) == total
	})
	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != total {
		t.Fatalf("delivered %d distinct messages, want %d", len(delivered), total)
	}
	for _, id := range ids {
		if n := delivered[id]; n != 1 {
			t.Errorf("%s delivered %d times, want exactly 1 with healthy leases", id, n)
		}
	}
}

// ---------- failure injection ----------

func TestCrashAfterBrokerAcceptanceCausesRedeliveryNotLoss(t *testing.T) {
	// Models the unavoidable window: the broker accepted the message but the
	// worker died before recording it. The message must be redelivered, never
	// lost.
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	var delivered atomic.Int64
	ackFails := atomic.Bool{}
	ackFails.Store(true)
	store.SetFault(func(op string) error {
		if op == "mark_published" && ackFails.Load() {
			return errors.New("connection reset by peer")
		}
		return nil
	})
	pub := outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		delivered.Add(1)
		return nil
	})
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, pub, cfg)

	waitFor(t, "the acknowledgement to fail", func() bool {
		var n int
		obs.snapshot(func(o *observer) { n = o.storeFails["publish_ack"] })
		return n >= 1
	})
	snap, _ := store.Get(ids[0])
	if snap.State == outbox.StatePublished {
		t.Fatal("message recorded as published even though the ack failed")
	}
	// The lease expires and the message is retried: a duplicate delivery.
	ackFails.Store(false)
	waitFor(t, "redelivery after lease expiry", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StatePublished
	})
	if n := delivered.Load(); n < 2 {
		t.Fatalf("deliveries = %d, want at least 2 (at-least-once)", n)
	}
}

func TestLeaseExpiryDuringPublishIsDetectedAndFenced(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()
	enqueue(t, store, outbox.Message{Type: "t"})

	// Worker A claims with a very short lease and stalls.
	a, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "a", Limit: 1, Lease: time.Millisecond})
	if err != nil || len(a.Messages) != 1 {
		t.Fatalf("claim: %+v %v", a, err)
	}
	time.Sleep(5 * time.Millisecond)
	// Worker B reclaims and publishes successfully.
	b, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "b", Limit: 1, Lease: time.Minute})
	if err != nil || len(b.Messages) != 1 {
		t.Fatalf("reclaim: %+v %v", b, err)
	}
	if err := store.MarkPublished(ctx, b.Messages[0].ID, b.Messages[0].LeaseEpoch); err != nil {
		t.Fatalf("new owner could not record publication: %v", err)
	}
	// Worker A finally finishes; its update must be rejected.
	if err := store.MarkPublished(ctx, a.Messages[0].ID, a.Messages[0].LeaseEpoch); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("stale ack = %v, want ErrLeaseLost", err)
	}
	snap, _ := store.Get(a.Messages[0].ID)
	if snap.State != outbox.StatePublished {
		t.Fatalf("state = %s, want published (the new owner's result)", snap.State)
	}
}

func TestWorkerSurvivesDatabaseOutage(t *testing.T) {
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	var down atomic.Bool
	down.Store(true)
	store.SetFault(func(op string) error {
		if down.Load() && op == "claim" {
			return errors.New("the database is away")
		}
		return nil
	})
	enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, &recorder{}, cfg)

	waitFor(t, "claim failures to be reported", func() bool {
		var n int
		obs.snapshot(func(o *observer) { n = o.storeFails["claim"] })
		return n >= 2
	})
	if store.CountState(outbox.StatePublished) != 0 {
		t.Fatal("something was published while the store was failing")
	}
	down.Store(false)
	waitFor(t, "recovery after the outage", func() bool {
		return store.CountState(outbox.StatePublished) == 1
	})
}

func TestWorkerReportsLeaseLostOnAcknowledgement(t *testing.T) {
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	store.SetFault(func(op string) error {
		if op == "mark_published" {
			return fmt.Errorf("%w: simulated", outbox.ErrLeaseLost)
		}
		return nil
	})
	enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, &recorder{}, cfg)

	waitFor(t, "lease-lost signal", func() bool {
		var n int
		obs.snapshot(func(o *observer) { n = o.leaseLost["publish_ack"] })
		return n >= 1
	})
	obs.snapshot(func(o *observer) {
		if o.storeFails["publish_ack"] != 0 {
			t.Error("a lost lease must not be reported as a store failure")
		}
	})
}

// ---------- operational features ----------

func TestWorkerSamplesStats(t *testing.T) {
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	cfg.StatsInterval = 5 * time.Millisecond
	blocked := make(chan struct{})
	pub := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		<-blocked
		return nil
	})
	for range 5 {
		enqueue(t, store, outbox.Message{Type: "t"})
	}
	startWorker(t, store, pub, cfg)

	waitFor(t, "a stats sample showing a backlog", func() bool {
		var ok bool
		obs.snapshot(func(o *observer) { ok = o.statSamples > 0 && o.lastStats.Pending+o.lastStats.Leased >= 3 })
		return ok
	})
	obs.snapshot(func(o *observer) {
		if o.lastStats.ObservedAt.IsZero() {
			t.Error("stats sample has no observation time")
		}
	})
	close(blocked)
}

func TestWorkerRetentionSweepOnlyDeletesPublished(t *testing.T) {
	store := memstore.New()
	cfg := testConfig()
	cfg.Retention = time.Millisecond
	cfg.PruneInterval = 5 * time.Millisecond
	cfg.PruneBatch = 10

	pendingFuture := outbox.Message{ID: outbox.NewID(), Type: "t", AvailableAt: time.Now().Add(time.Hour)}
	enqueue(t, store, pendingFuture)
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, &recorder{}, cfg)

	waitFor(t, "published message to be pruned", func() bool {
		_, ok := store.Get(ids[0])
		return !ok
	})
	if _, ok := store.Get(pendingFuture.ID); !ok {
		t.Fatal("the retention sweep deleted a pending message")
	}
}

func TestWorkerWarnsWhenRetentionIsConfiguredWithoutPruner(t *testing.T) {
	// A store that cannot prune must not silently look like it is pruning.
	store := bareStore{memstore.New()}
	cfg := testConfig()
	cfg.Retention = time.Hour
	var buf safeBuffer
	cfg.Logger = slog.New(slog.NewTextHandler(&buf, nil))
	w, err := outbox.NewWorker(store, &recorder{}, cfg)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Shutdown(context.Background())
	if got := buf.String(); !contains(got, "Pruner") {
		t.Fatalf("logs do not mention the missing Pruner: %s", got)
	}
}

func TestNotifyWakesThePollerEarly(t *testing.T) {
	store := memstore.New()
	cfg := testConfig()
	cfg.PollInterval = 30 * time.Second // only Notify can make this test pass
	published := make(chan struct{})
	var once sync.Once
	pub := outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		once.Do(func() { close(published) })
		return nil
	})
	w := startWorker(t, store, pub, cfg)
	time.Sleep(20 * time.Millisecond) // let the poller settle into its sleep
	enqueue(t, store, outbox.Message{Type: "t"})
	w.Notify()

	select {
	case <-published:
	case <-time.After(3 * time.Second):
		t.Fatal("Notify did not wake the poller")
	}
	for range 10 {
		w.Notify() // must never block, even unobserved
	}
}

// ---------- small test doubles ----------

// bareStore exposes only the outbox.Store interface, hiding the optional
// Pruner and Inspector capabilities that memstore also implements.
type bareStore struct{ s *memstore.Store }

func (b bareStore) Claim(ctx context.Context, r outbox.ClaimRequest) (outbox.ClaimResult, error) {
	return b.s.Claim(ctx, r)
}
func (b bareStore) MarkPublished(ctx context.Context, id outbox.ID, epoch int64) error {
	return b.s.MarkPublished(ctx, id, epoch)
}
func (b bareStore) Reschedule(ctx context.Context, id outbox.ID, epoch int64, d time.Duration, f outbox.Failure) error {
	return b.s.Reschedule(ctx, id, epoch, d, f)
}
func (b bareStore) Fail(ctx context.Context, id outbox.ID, epoch int64, f outbox.Failure) error {
	return b.s.Fail(ctx, id, epoch, f)
}
func (b bareStore) Release(ctx context.Context, id outbox.ID, epoch int64) error {
	return b.s.Release(ctx, id, epoch)
}
func (b bareStore) Stats(ctx context.Context) (outbox.Stats, error) { return b.s.Stats(ctx) }

type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ---------- robustness ----------

func TestPublisherPanicIsContainedAndFailsTheMessage(t *testing.T) {
	store := memstore.New()
	obs := newObserver()
	cfg := testConfig()
	cfg.Observer = obs
	var buf safeBuffer
	cfg.Logger = slog.New(slog.NewTextHandler(&buf, nil))

	ids := enqueue(t, store, outbox.Message{Type: "poison"}, outbox.Message{Type: "healthy"})
	var healthy atomic.Int64
	startWorker(t, store, outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		if m.Type == "poison" {
			panic("a malformed payload took the serialiser with it")
		}
		healthy.Add(1)
		return nil
	}), cfg)

	// The panicking message fails permanently instead of taking the process
	// down, and it is not retried.
	waitFor(t, "the panicking message to fail", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StateFailed
	})
	snap, _ := store.Get(ids[0])
	if snap.Attempts != 1 {
		t.Errorf("attempts = %d, want 1: a panic must not be retried", snap.Attempts)
	}
	if !contains(snap.LastError, "panicked") {
		t.Errorf("stored error %q does not mention the panic", snap.LastError)
	}

	// The worker keeps working: its slot was not leaked.
	waitFor(t, "the healthy message to publish", func() bool {
		snap, _ := store.Get(ids[1])
		return snap.State == outbox.StatePublished
	})
	for range 20 {
		enqueue(t, store, outbox.Message{Type: "healthy"})
	}
	waitFor(t, "later messages to publish", func() bool { return healthy.Load() >= 21 })

	if logs := buf.String(); !contains(logs, "panicked") || !contains(logs, "stack") {
		t.Errorf("the panic was not logged with a stack: %s", logs)
	}
	obs.snapshot(func(o *observer) {
		if o.failures[outbox.ReasonPermanent] != 1 {
			t.Errorf("permanent failures = %d, want 1", o.failures[outbox.ReasonPermanent])
		}
	})
}

func TestDelayedMessagesAreNotPublishedEarly(t *testing.T) {
	store := memstore.New()
	var published atomic.Int64
	future := outbox.Message{ID: outbox.NewID(), Type: "later", AvailableAt: time.Now().Add(time.Hour)}
	soon := outbox.Message{ID: outbox.NewID(), Type: "soon", AvailableAt: time.Now().Add(50 * time.Millisecond)}
	enqueue(t, store, future, soon)
	startWorker(t, store, outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		published.Add(1)
		return nil
	}), testConfig())

	waitFor(t, "the near-future message to publish", func() bool {
		snap, _ := store.Get(soon.ID)
		return snap.State == outbox.StatePublished
	})
	if snap, _ := store.Get(future.ID); snap.State != outbox.StatePending {
		t.Fatalf("the hour-away message was published early: %+v", snap)
	}
	if n := published.Load(); n != 1 {
		t.Fatalf("published %d messages, want 1", n)
	}
}

func TestClassifyHookOverridesClassification(t *testing.T) {
	store := memstore.New()
	cfg := testConfig()
	// A custom classifier that treats a broker's own error as permanent
	// without the publisher having to wrap it.
	brokerErr := errors.New("UNKNOWN_TOPIC_OR_PARTITION")
	cfg.Classify = func(err error) outbox.Disposition {
		if errors.Is(err, brokerErr) {
			return outbox.PermanentFailure
		}
		return outbox.Classify(err)
	}
	var calls atomic.Int64
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		calls.Add(1)
		return brokerErr
	}), cfg)

	waitFor(t, "the custom classifier to fail the message", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StateFailed
	})
	time.Sleep(50 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("publisher called %d times, want 1", n)
	}
}

func TestErrorSummaryHookCanRedact(t *testing.T) {
	store := memstore.New()
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.ErrorSummary = func(error) string { return "[redacted]" }
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		return errors.New("postgres://user:hunter2@db.internal/app failed")
	}), cfg)

	waitFor(t, "the message to fail", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StateFailed
	})
	snap, _ := store.Get(ids[0])
	if snap.LastError != "[redacted]" {
		t.Fatalf("stored error = %q, want the redacted value", snap.LastError)
	}
}

func TestPublishTimeoutIsEnforcedAndRetried(t *testing.T) {
	store := memstore.New()
	cfg := testConfig()
	cfg.PublishTimeout = 30 * time.Millisecond
	cfg.StoreTimeout = 100 * time.Millisecond
	cfg.LeaseDuration = time.Second

	var attempts atomic.Int64
	ids := enqueue(t, store, outbox.Message{Type: "t"})
	startWorker(t, store, outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		if attempts.Add(1) == 1 {
			<-ctx.Done() // too slow on the first attempt
			return ctx.Err()
		}
		return nil
	}), cfg)

	waitFor(t, "the timed-out attempt to be retried and succeed", func() bool {
		snap, _ := store.Get(ids[0])
		return snap.State == outbox.StatePublished
	})
	snap, _ := store.Get(ids[0])
	if snap.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2: a publish timeout is a retryable failure", snap.Attempts)
	}
}

func TestTracerIsInvokedPerAttempt(t *testing.T) {
	store := memstore.New()
	cfg := testConfig()
	tr := &recordingTracer{}
	cfg.Tracer = tr
	enqueue(t, store, outbox.Message{Type: "t"}, outbox.Message{Type: "t"})
	startWorker(t, store, &recorder{}, cfg)

	waitFor(t, "both messages published", func() bool {
		return store.CountState(outbox.StatePublished) == 2
	})
	waitFor(t, "both spans to end", func() bool { return tr.ended() == 2 })
	if started := tr.started(); started != 2 {
		t.Fatalf("started %d spans, want 2", started)
	}
	if errs := tr.errors(); errs != 0 {
		t.Fatalf("%d spans ended with an error, want 0", errs)
	}
}

type recordingTracer struct {
	mu            sync.Mutex
	begun, done   int
	failed        int
	sawPublishCtx bool
}

func (t *recordingTracer) StartPublish(ctx context.Context, m *outbox.Claimed) (context.Context, outbox.EndSpan) {
	t.mu.Lock()
	t.begun++
	t.mu.Unlock()
	type key struct{}
	ctx = context.WithValue(ctx, key{}, m.ID)
	return ctx, func(err error) {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.done++
		if err != nil {
			t.failed++
		}
	}
}

func (t *recordingTracer) started() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.begun
}

func (t *recordingTracer) ended() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.done
}

func (t *recordingTracer) errors() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failed
}
