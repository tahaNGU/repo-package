package memstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tahaNGU/gocast"
	"github.com/tahaNGU/gocast/memstore"
)

func TestTransactionIsolation(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	tx := s.Begin()
	id := outbox.NewID()
	if err := s.Enqueue(ctx, tx, outbox.Message{ID: id, Type: "t"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, ok := s.Get(id); ok {
		t.Fatal("an uncommitted message is visible")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, ok := s.Get(id); !ok {
		t.Fatal("a committed message is not visible")
	}
	if err := tx.Commit(); err == nil {
		t.Error("committing twice was accepted")
	}
	if err := tx.Rollback(); err != nil {
		t.Errorf("rollback after commit = %v, want nil (the deferred-rollback idiom)", err)
	}
}

func TestClockIsInjectable(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	s.SetClock(func() time.Time { return now })

	id := outbox.NewID()
	if err := s.Enqueue(ctx, nil, outbox.Message{ID: id, Type: "t"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	res, err := s.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Minute})
	if err != nil || len(res.Messages) != 1 {
		t.Fatalf("claim = %+v, %v", res, err)
	}
	if !res.Messages[0].LeaseExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("lease expiry = %s, want %s", res.Messages[0].LeaseExpiresAt, now.Add(time.Minute))
	}
	// Nothing is claimable until the lease expires on the injected clock.
	empty, err := s.Claim(ctx, outbox.ClaimRequest{Owner: "w2", Limit: 1, Lease: time.Minute})
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("claimed %d messages while leased", len(empty.Messages))
	}
	now = now.Add(2 * time.Minute)
	again, err := s.Claim(ctx, outbox.ClaimRequest{Owner: "w2", Limit: 1, Lease: time.Minute})
	if err != nil || len(again.Messages) != 1 || again.ExpiredLeases != 1 {
		t.Fatalf("reclaim after the lease expired = %+v, %v", again, err)
	}
}

func TestFaultInjection(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	boom := errors.New("database is away")
	var failing string
	s.SetFault(func(op string) error {
		if op == failing {
			return boom
		}
		return nil
	})
	id := outbox.NewID()
	failing = "enqueue"
	if err := s.Enqueue(ctx, nil, outbox.Message{ID: id, Type: "t"}); !errors.Is(err, boom) {
		t.Fatalf("enqueue = %v, want the injected error", err)
	}
	failing = ""
	if err := s.Enqueue(ctx, nil, outbox.Message{ID: id, Type: "t"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	for _, op := range []string{"claim", "stats", "prune", "list_failed", "requeue"} {
		failing = op
		var err error
		switch op {
		case "claim":
			_, err = s.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Minute})
		case "stats":
			_, err = s.Stats(ctx)
		case "prune":
			_, err = s.DeletePublished(ctx, time.Hour, 10)
		case "list_failed":
			_, err = s.ListFailed(ctx, 10, outbox.ID{})
		case "requeue":
			_, err = s.Requeue(ctx, id)
		}
		if !errors.Is(err, boom) {
			t.Errorf("%s = %v, want the injected error", op, err)
		}
	}
	failing = ""
	s.SetFault(nil)
	if _, err := s.Stats(ctx); err != nil {
		t.Errorf("clearing the fault did not help: %v", err)
	}
}

func TestContextCancellationIsHonoured(t *testing.T) {
	s := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	id := outbox.NewID()
	checks := map[string]error{
		"enqueue":        s.Enqueue(ctx, nil, outbox.Message{ID: id, Type: "t"}),
		"mark_published": s.MarkPublished(ctx, id, 1),
		"reschedule":     s.Reschedule(ctx, id, 1, time.Second, outbox.Failure{}),
		"fail":           s.Fail(ctx, id, 1, outbox.Failure{}),
		"release":        s.Release(ctx, id, 1),
	}
	for op, err := range checks {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a cancelled context = %v, want context.Canceled", op, err)
		}
	}
	if _, err := s.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Second}); !errors.Is(err, context.Canceled) {
		t.Errorf("claim with a cancelled context = %v", err)
	}
	if _, err := s.Stats(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("stats with a cancelled context = %v", err)
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				id := outbox.NewID()
				if err := s.Enqueue(ctx, nil, outbox.Message{ID: id, Type: "t"}); err != nil {
					t.Error(err)
					return
				}
				res, err := s.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 4, Lease: time.Minute})
				if err != nil {
					t.Error(err)
					return
				}
				for _, m := range res.Messages {
					if err := s.MarkPublished(ctx, m.ID, m.LeaseEpoch); err != nil {
						t.Error(err)
						return
					}
				}
				if _, err := s.Stats(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if n := s.CountState(outbox.StatePublished); n == 0 {
		t.Fatal("nothing was published")
	}
	if s.Len() != 400 {
		t.Fatalf("stored %d messages, want 400", s.Len())
	}
}

func TestNotFoundAndLeaseLost(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	if err := s.MarkPublished(ctx, outbox.NewID(), 1); !errors.Is(err, outbox.ErrNotFound) {
		t.Errorf("unknown id = %v, want ErrNotFound", err)
	}
	id := outbox.NewID()
	if err := s.Enqueue(ctx, nil, outbox.Message{ID: id, Type: "t"}); err != nil {
		t.Fatal(err)
	}
	// Not leased at all.
	if err := s.MarkPublished(ctx, id, 0); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Errorf("pending message = %v, want ErrLeaseLost", err)
	}
	res, err := s.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkPublished(ctx, id, res.Messages[0].LeaseEpoch+1); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Errorf("wrong epoch = %v, want ErrLeaseLost", err)
	}
	if err := s.MarkPublished(ctx, id, res.Messages[0].LeaseEpoch); err != nil {
		t.Errorf("correct epoch = %v, want success", err)
	}
}

func TestDeletePublishedOnlyRemovesPublished(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	now := time.Now().UTC()
	s.SetClock(func() time.Time { return now })

	published, pending := outbox.NewID(), outbox.NewID()
	if err := s.Enqueue(ctx, nil,
		outbox.Message{ID: published, Type: "t"},
		outbox.Message{ID: pending, Type: "t"}); err != nil {
		t.Fatal(err)
	}
	res, err := s.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Minute})
	if err != nil || len(res.Messages) != 1 {
		t.Fatalf("claim: %v", err)
	}
	// Publish whichever was claimed first, then age the clock.
	claimedID := res.Messages[0].ID
	if err := s.MarkPublished(ctx, claimedID, res.Messages[0].LeaseEpoch); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	n, err := s.DeletePublished(ctx, time.Hour, 10)
	if err != nil || n != 1 {
		t.Fatalf("DeletePublished = %d, %v; want 1", n, err)
	}
	if s.Len() != 1 {
		t.Fatalf("%d messages left, want the unpublished one", s.Len())
	}
	if _, ok := s.Get(claimedID); ok {
		t.Error("the published message was not deleted")
	}
}
