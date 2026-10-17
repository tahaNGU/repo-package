package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tahaNGU/gocast"
	"github.com/tahaNGU/gocast/postgres"
)

func workerConfig() outbox.Config {
	return outbox.Config{
		Concurrency:    4,
		BatchSize:      32,
		PollInterval:   5 * time.Millisecond,
		LeaseDuration:  5 * time.Second,
		PublishTimeout: 2 * time.Second,
		StoreTimeout:   2 * time.Second,
		MaxAttempts:    5,
		Backoff:        outbox.Backoff{Initial: 5 * time.Millisecond, Max: 50 * time.Millisecond, Multiplier: 2},
		StatsInterval:  -1,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func startWorker(t *testing.T, store outbox.Store, pub outbox.Publisher, cfg outbox.Config) *outbox.Worker {
	t.Helper()
	w, err := outbox.NewWorker(store, pub, cfg)
	requireNoError(t, err, "NewWorker")
	requireNoError(t, w.Start(context.Background()), "Start")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = w.Shutdown(ctx)
	})
	return w
}

// deliveries records what a publisher was given, per message id.
type deliveries struct {
	mu    sync.Mutex
	count map[outbox.ID]int
	order []outbox.Message
}

func newDeliveries() *deliveries {
	return &deliveries{count: map[outbox.ID]int{}}
}

func (d *deliveries) record(m outbox.Message) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.count[m.ID]++
	d.order = append(d.order, m)
}

func (d *deliveries) total() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.order)
}

func (d *deliveries) distinct() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.count)
}

func TestEndToEndPublishesEveryCommittedMessage(t *testing.T) {
	store, _ := newStore(t)
	const total = 200
	msgs := make([]outbox.Message, total)
	for i := range msgs {
		msgs[i] = outbox.Message{
			Type:        "orders.created.v1",
			Payload:     []byte(fmt.Sprintf(`{"order":%d}`, i)),
			ContentType: "application/json",
			Headers:     map[string]string{"tenant": "acme"},
		}
	}
	ids := enqueueCommitted(t, store, msgs...)

	got := newDeliveries()
	startWorker(t, store, outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		got.record(m)
		return nil
	}), workerConfig())

	waitFor(t, "every message to be published", func() bool {
		return countState(t, store, outbox.StatePublished) == total
	})
	if got.distinct() != total {
		t.Fatalf("delivered %d distinct messages, want %d", got.distinct(), total)
	}
	if got.total() != total {
		t.Errorf("delivered %d times in total, want %d: healthy leases must not duplicate", got.total(), total)
	}
	for _, id := range ids {
		r := readRow(t, store, id)
		if r.State != string(outbox.StatePublished) || r.Attempts != 1 || !r.PublishedAt.Valid || r.LeaseOwner.Valid {
			t.Fatalf("%s: row = %+v", id, r)
		}
	}
}

func TestManyWorkersShareOneTableWithoutDuplicating(t *testing.T) {
	store, _ := newStore(t)
	const total = 500
	msgs := make([]outbox.Message, total)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t", Payload: []byte(fmt.Sprint(i))}
	}
	enqueueCommitted(t, store, msgs...)

	got := newDeliveries()
	pub := outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		got.record(m)
		return nil
	})
	// Six independent workers, as six application replicas would be.
	for i := range 6 {
		cfg := workerConfig()
		cfg.Name = fmt.Sprintf("replica-%d", i)
		startWorker(t, store, pub, cfg)
	}
	waitFor(t, "the backlog to drain", func() bool {
		return countState(t, store, outbox.StatePublished) == total
	})
	if got.distinct() != total {
		t.Fatalf("delivered %d distinct messages, want %d", got.distinct(), total)
	}
	if got.total() != total {
		t.Errorf("delivered %d times in total, want exactly %d", got.total(), total)
	}
	var owners int
	err := sharedDB.QueryRowContext(context.Background(),
		`SELECT count(DISTINCT lease_owner) FROM `+store.Table()).Scan(&owners)
	requireNoError(t, err, "count owners")
}

// shortLeaseStore hands out leases far shorter than the worker asked for,
// which is how a stalled worker or a long GC pause looks from the database's
// point of view: the lease expires while the publisher is still running.
type shortLeaseStore struct {
	*postgres.Store
	lease time.Duration
}

func (s shortLeaseStore) Claim(ctx context.Context, req outbox.ClaimRequest) (outbox.ClaimResult, error) {
	req.Lease = s.lease
	return s.Store.Claim(ctx, req)
}

func TestLeaseExpiryDuringPublishIsFencedAndRedelivered(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"})

	// Worker A claims with a lease that expires almost immediately, and its
	// publish blocks until we let it go.
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	slow := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	cfgA := workerConfig()
	cfgA.Name = "stalled"
	cfgA.Concurrency = 1
	startWorker(t, shortLeaseStore{store, 200 * time.Millisecond}, slow, cfgA)
	<-entered

	// Worker B reclaims the expired lease and publishes it for real.
	got := newDeliveries()
	cfgB := workerConfig()
	cfgB.Name = "healthy"
	startWorker(t, store, outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		got.record(m)
		return nil
	}), cfgB)

	waitFor(t, "the healthy worker to publish", func() bool {
		return countState(t, store, outbox.StatePublished) == 1
	})
	r := readRow(t, store, ids[0])
	if r.Attempts < 2 {
		t.Errorf("attempts = %d, want at least 2 (the stalled attempt counts)", r.Attempts)
	}
	epochAfter := r.LeaseEpoch

	// Worker A now finishes. Its acknowledgement must be rejected, leaving the
	// healthy worker's result in place.
	close(release)
	time.Sleep(300 * time.Millisecond)
	r = readRow(t, store, ids[0])
	if r.State != string(outbox.StatePublished) {
		t.Fatalf("state = %s, want published", r.State)
	}
	if r.LeaseEpoch != epochAfter {
		t.Errorf("lease epoch moved from %d to %d: a stale worker changed the row", epochAfter, r.LeaseEpoch)
	}
	if _, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "x", Limit: 10, Lease: time.Minute}); err != nil {
		t.Fatalf("claim after the race: %v", err)
	}
}

func TestCrashedWorkerMessagesAreRedeliveredAfterLeaseExpiry(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"})

	// A worker claims the message and then "crashes": no acknowledgement, no
	// release, nothing. This is the state its row is left in.
	claimed, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "crashed", Limit: 1, Lease: 500 * time.Millisecond})
	requireNoError(t, err, "claim")
	if len(claimed.Messages) != 1 {
		t.Fatalf("claimed %d messages", len(claimed.Messages))
	}
	if r := readRow(t, store, ids[0]); r.State != string(outbox.StateLeased) {
		t.Fatalf("row = %+v, want leased", r)
	}

	got := newDeliveries()
	startWorker(t, store, outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		got.record(m)
		return nil
	}), workerConfig())

	waitFor(t, "recovery of the crashed worker's message", func() bool {
		return countState(t, store, outbox.StatePublished) == 1
	})
	if got.distinct() != 1 {
		t.Fatalf("delivered %d messages, want 1", got.distinct())
	}
	r := readRow(t, store, ids[0])
	if r.Attempts != 2 {
		t.Errorf("attempts = %d, want 2: the crashed attempt is counted", r.Attempts)
	}
	if r.LeaseEpoch != claimed.Messages[0].LeaseEpoch+1 {
		t.Errorf("lease epoch = %d, want %d", r.LeaseEpoch, claimed.Messages[0].LeaseEpoch+1)
	}
	// The crashed worker, if it ever comes back, cannot touch the row.
	err = store.MarkPublished(ctx, ids[0], claimed.Messages[0].LeaseEpoch)
	if !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("the resurrected worker's ack = %v, want ErrLeaseLost", err)
	}
}

func TestRetryScheduleSurvivesAProcessRestart(t *testing.T) {
	store, _ := newStore(t)
	cfg := workerConfig()
	cfg.Backoff = outbox.Backoff{Initial: time.Hour, Max: time.Hour, Multiplier: 1}
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"})

	var attempts atomic.Int64
	failing := outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		attempts.Add(1)
		return errors.New("broker down")
	})
	w, err := outbox.NewWorker(store, failing, cfg)
	requireNoError(t, err, "NewWorker")
	requireNoError(t, w.Start(context.Background()), "Start")
	waitFor(t, "the first attempt to fail", func() bool {
		return readRow(t, store, ids[0]).Attempts == 1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	requireNoError(t, w.Shutdown(ctx), "Shutdown")

	before := readRow(t, store, ids[0])
	if before.State != string(outbox.StatePending) {
		t.Fatalf("state = %s, want pending", before.State)
	}
	if until := time.Until(before.AvailableAt); until < 30*time.Minute {
		t.Fatalf("available_at is %s away, want about an hour", until)
	}

	// Restart: a brand new worker, as a redeployed process would be.
	startWorker(t, store, failing, cfg)
	time.Sleep(200 * time.Millisecond)
	after := readRow(t, store, ids[0])
	if after.Attempts != 1 {
		t.Fatalf("attempts = %d after restart, want the stored backoff to hold at 1", after.Attempts)
	}
	if !after.AvailableAt.Equal(before.AvailableAt) {
		t.Fatalf("available_at changed across the restart: %s -> %s", before.AvailableAt, after.AvailableAt)
	}
}

func TestExhaustedMessagesBecomeFailedAndAreRecoverable(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	cfg := workerConfig()
	cfg.MaxAttempts = 3
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"})

	var fail atomic.Bool
	fail.Store(true)
	var calls atomic.Int64
	pub := outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		calls.Add(1)
		if fail.Load() {
			return errors.New("broker down")
		}
		return nil
	})
	startWorker(t, store, pub, cfg)

	waitFor(t, "the retry budget to run out", func() bool {
		return readRow(t, store, ids[0]).State == string(outbox.StateFailed)
	})
	r := readRow(t, store, ids[0])
	if r.Attempts != 3 || r.ErrorKind.String != string(outbox.ReasonExhausted) {
		t.Fatalf("row = %+v, want 3 attempts and an exhausted summary", r)
	}
	listed, err := store.ListFailed(ctx, 10, outbox.ID{})
	requireNoError(t, err, "list failed")
	if len(listed) != 1 || listed[0].ID != ids[0] {
		t.Fatalf("listed = %+v", listed)
	}

	fail.Store(false)
	n, err := store.Requeue(ctx, ids[0])
	requireNoError(t, err, "requeue")
	if n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	waitFor(t, "the requeued message to publish", func() bool {
		return readRow(t, store, ids[0]).State == string(outbox.StatePublished)
	})
}

func TestPermanentFailureIsNotRetried(t *testing.T) {
	store, _ := newStore(t)
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"})
	var calls atomic.Int64
	startWorker(t, store, outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		calls.Add(1)
		return outbox.Permanent(errors.New("unknown topic"))
	}), workerConfig())

	waitFor(t, "a permanent failure", func() bool {
		return readRow(t, store, ids[0]).State == string(outbox.StateFailed)
	})
	time.Sleep(200 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("publisher called %d times, want 1", n)
	}
	r := readRow(t, store, ids[0])
	if r.ErrorKind.String != string(outbox.ReasonPermanent) || !contains(r.LastError.String, "unknown topic") {
		t.Fatalf("row = %+v", r)
	}
}

func TestGracefulShutdownPublishesOrReleasesEveryClaimedMessage(t *testing.T) {
	store, _ := newStore(t)
	const total = 100
	msgs := make([]outbox.Message, total)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t", Payload: []byte(fmt.Sprint(i))}
	}
	enqueueCommitted(t, store, msgs...)

	got := newDeliveries()
	cfg := workerConfig()
	cfg.Concurrency = 8
	pub := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		time.Sleep(3 * time.Millisecond)
		got.record(m)
		return nil
	})
	w, err := outbox.NewWorker(store, pub, cfg)
	requireNoError(t, err, "NewWorker")
	requireNoError(t, w.Start(context.Background()), "Start")

	// Stop in the middle of the backlog.
	waitFor(t, "publishing to start", func() bool { return got.total() > 5 })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	requireNoError(t, w.Shutdown(ctx), "Shutdown")

	// Nothing may be left leased: every claimed message was either published
	// or handed back.
	if leased := countState(t, store, outbox.StateLeased); leased != 0 {
		t.Fatalf("%d messages left leased after a graceful shutdown", leased)
	}
	published := countState(t, store, outbox.StatePublished)
	pending := countState(t, store, outbox.StatePending)
	if published+pending != total {
		t.Fatalf("published %d + pending %d != %d", published, pending, total)
	}
	if published == 0 || published == total {
		t.Logf("published %d of %d before shutdown", published, total)
	}

	// A new worker finishes the job, and no message is published twice.
	startWorker(t, store, outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		got.record(m)
		return nil
	}), workerConfig())
	waitFor(t, "the rest of the backlog", func() bool {
		return countState(t, store, outbox.StatePublished) == total
	})
	if got.distinct() != total {
		t.Fatalf("delivered %d distinct messages, want %d", got.distinct(), total)
	}
	if got.total() != total {
		t.Errorf("delivered %d times in total, want %d: a clean shutdown should not duplicate", got.total(), total)
	}
}

func TestWorkerRetentionSweepDeletesOnlyPublishedRows(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	future := outbox.Message{ID: outbox.NewID(), Type: "later", AvailableAt: time.Now().Add(time.Hour)}
	enqueueCommitted(t, store, future)
	ids := enqueueCommitted(t, store, outbox.Message{Type: "now"})

	cfg := workerConfig()
	cfg.Retention = time.Second
	cfg.PruneInterval = 20 * time.Millisecond
	startWorker(t, store, outbox.PublisherFunc(func(context.Context, outbox.Message) error { return nil }), cfg)

	waitFor(t, "publication", func() bool { return countState(t, store, outbox.StatePublished) == 1 })
	_, err := db.ExecContext(ctx, `UPDATE `+store.Table()+` SET published_at = now() - interval '1 hour' WHERE id = $1`, ids[0])
	requireNoError(t, err, "age the published row")

	waitFor(t, "the retention sweep", func() bool {
		var n int
		requireNoError(t, db.QueryRowContext(ctx,
			`SELECT count(*) FROM `+store.Table()+` WHERE id = $1`, ids[0]).Scan(&n), "count")
		return n == 0
	})
	var n int
	requireNoError(t, db.QueryRowContext(ctx,
		`SELECT count(*) FROM `+store.Table()+` WHERE id = $1`, future.ID).Scan(&n), "count")
	if n != 1 {
		t.Fatal("the retention sweep deleted a pending message")
	}
}

func TestStatsSamplingReportsRealDepth(t *testing.T) {
	store, _ := newStore(t)
	msgs := make([]outbox.Message, 20)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t"}
	}
	enqueueCommitted(t, store, msgs...)

	var (
		mu   sync.Mutex
		last outbox.Stats
		seen bool
	)
	cfg := workerConfig()
	cfg.StatsInterval = 10 * time.Millisecond
	cfg.Observer = statsObserver{fn: func(s outbox.Stats) {
		mu.Lock()
		defer mu.Unlock()
		last, seen = s, true
	}}
	block := make(chan struct{})
	startWorker(t, store, outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		<-block
		return nil
	}), cfg)

	waitFor(t, "a stats sample", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seen && last.Pending+last.Leased >= 20
	})
	mu.Lock()
	got := last
	mu.Unlock()
	if got.OldestPendingAge <= 0 {
		t.Errorf("oldest pending age = %s, want a positive lag with a stalled publisher", got.OldestPendingAge)
	}
	if got.ObservedAt.IsZero() {
		t.Error("stats sample has no observation time")
	}
	close(block)
}

type statsObserver struct {
	outbox.NopObserver
	fn func(outbox.Stats)
}

func (o statsObserver) StatsSampled(_ context.Context, s outbox.Stats) { o.fn(s) }

// ---------- ordered delivery ----------

func TestOrderedDeliveryKeepsEachKeyInSequence(t *testing.T) {
	store, _ := newStore(t, postgres.WithOrdering())
	const keys, perKey = 10, 20

	var msgs []outbox.Message
	for k := range keys {
		for i := range perKey {
			msgs = append(msgs, outbox.Message{
				Type:        "t",
				OrderingKey: fmt.Sprintf("key-%d", k),
				Payload:     []byte(fmt.Sprint(i)),
			})
		}
	}
	enqueueCommitted(t, store, msgs...)

	var mu sync.Mutex
	seen := map[string][]string{}
	pub := outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		// Make the window for a reordering bug as wide as possible.
		time.Sleep(time.Millisecond)
		mu.Lock()
		seen[m.OrderingKey] = append(seen[m.OrderingKey], string(m.Payload))
		mu.Unlock()
		return nil
	})
	for i := range 4 { // several workers competing for the same keys
		cfg := workerConfig()
		cfg.Name = fmt.Sprintf("ordered-%d", i)
		cfg.Concurrency = 4
		startWorker(t, store, pub, cfg)
	}
	waitFor(t, "every keyed message to be published", func() bool {
		return countState(t, store, outbox.StatePublished) == keys*perKey
	})

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != keys {
		t.Fatalf("saw %d keys, want %d", len(seen), keys)
	}
	for key, got := range seen {
		if len(got) != perKey {
			t.Errorf("%s: %d deliveries, want %d", key, len(got), perKey)
			continue
		}
		want := make([]string, perKey)
		for i := range want {
			want[i] = fmt.Sprint(i)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("%s: delivery order = %v, want %v", key, got, want)
				break
			}
		}
	}
}

func TestOrderedDeliveryDoesNotSerialiseDifferentKeys(t *testing.T) {
	store, _ := newStore(t, postgres.WithOrdering())
	var msgs []outbox.Message
	for k := range 8 {
		msgs = append(msgs, outbox.Message{Type: "t", OrderingKey: fmt.Sprintf("k%d", k)})
	}
	enqueueCommitted(t, store, msgs...)

	var concurrent, peak atomic.Int64
	pub := outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		n := concurrent.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		concurrent.Add(-1)
		return nil
	})
	cfg := workerConfig()
	cfg.Concurrency = 8
	startWorker(t, store, pub, cfg)
	waitFor(t, "all keys published", func() bool {
		return countState(t, store, outbox.StatePublished) == 8
	})
	if peak.Load() < 2 {
		t.Fatalf("peak concurrency = %d: distinct keys must publish in parallel", peak.Load())
	}
}

func TestOrderedDeliveryHoldsAKeyBehindAFailedMessage(t *testing.T) {
	store, _ := newStore(t, postgres.WithOrdering())
	ctx := context.Background()
	ids := enqueueCommitted(t, store,
		outbox.Message{Type: "poison", OrderingKey: "k"},
		outbox.Message{Type: "follower", OrderingKey: "k"},
		outbox.Message{Type: "other", OrderingKey: "other"},
	)
	var poisonous atomic.Bool
	poisonous.Store(true)
	var mu sync.Mutex
	var order []string
	cfg := workerConfig()
	cfg.MaxAttempts = 1
	startWorker(t, store, outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		if m.Type == "poison" && poisonous.Load() {
			return outbox.Permanent(errors.New("nope"))
		}
		mu.Lock()
		order = append(order, m.Type)
		mu.Unlock()
		return nil
	}), cfg)

	waitFor(t, "the poison message to fail", func() bool {
		return readRow(t, store, ids[0]).State == string(outbox.StateFailed)
	})
	// A different key is unaffected by the failure.
	waitFor(t, "the unrelated key to publish", func() bool {
		return readRow(t, store, ids[2]).State == string(outbox.StatePublished)
	})
	// The follower stays put: ordered delivery means a failed message blocks
	// its key until an operator deals with it.
	time.Sleep(300 * time.Millisecond)
	if r := readRow(t, store, ids[1]); r.State != string(outbox.StatePending) || r.Attempts != 0 {
		t.Fatalf("follower = %+v, want an untouched pending row behind the failed one", r)
	}

	// Once the cause is fixed and the message requeued, the key drains in
	// order: the recovered message first, then its follower.
	poisonous.Store(false)
	n, err := store.Requeue(ctx, ids[0])
	requireNoError(t, err, "requeue")
	if n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	waitFor(t, "the key to drain after recovery", func() bool {
		return readRow(t, store, ids[1]).State == string(outbox.StatePublished)
	})
	mu.Lock()
	defer mu.Unlock()
	var keyOrder []string
	for _, typ := range order {
		if typ == "poison" || typ == "follower" {
			keyOrder = append(keyOrder, typ)
		}
	}
	if len(keyOrder) != 2 || keyOrder[0] != "poison" || keyOrder[1] != "follower" {
		t.Fatalf("delivery order for the key = %v, want [poison follower]", keyOrder)
	}
}
