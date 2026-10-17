package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tahaNGU/gocast"
)

// These benchmarks isolate the library from any database: they measure id
// generation, backoff, serialisation and the worker's own pipeline. The
// database-backed numbers are in the postgres package.

func BenchmarkNewID(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = outbox.NewID()
	}
}

func BenchmarkIDString(b *testing.B) {
	id := outbox.NewID()
	b.ReportAllocs()
	for b.Loop() {
		_ = id.String()
	}
}

func BenchmarkBackoffDelay(b *testing.B) {
	bo := outbox.DefaultBackoff()
	b.ReportAllocs()
	for b.Loop() {
		_ = bo.Delay(5)
	}
}

// BenchmarkPayloadSerialisation shows what the application pays to turn a
// domain event into the bytes the outbox stores. The outbox itself never
// parses a payload, so this cost is entirely the caller's choice of encoding.
func BenchmarkPayloadSerialisation(b *testing.B) {
	type item struct {
		SKU   string `json:"sku"`
		Qty   int    `json:"qty"`
		Price int    `json:"price"`
	}
	type event struct {
		OrderID    string `json:"order_id"`
		CustomerID string `json:"customer_id"`
		Status     string `json:"status"`
		Items      []item `json:"items"`
	}
	ev := event{OrderID: outbox.NewID().String(), CustomerID: "cust-99812", Status: "CREATED"}
	for i := range 3 {
		ev.Items = append(ev.Items, item{SKU: fmt.Sprintf("SKU-%06d", i), Qty: i + 1, Price: 199 * (i + 1)})
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(ev); err != nil {
			b.Fatal(err)
		}
	}
}

func benchPayload() []byte {
	var sb strings.Builder
	sb.WriteString(`{"order_id":"01924f1e-2b3c-7d4e-8f90-a1b2c3d4e5f6","items":[`)
	for i := range 3 {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"sku":"SKU-%06d","qty":%d}`, i, i+1)
	}
	sb.WriteString(`]}`)
	return []byte(sb.String())
}

// fastStore is a store with no bookkeeping cost at all: Claim hands out
// preloaded messages from a slice and the acknowledgement paths do nothing.
// It exists so BenchmarkWorkerPipeline measures the worker's own overhead --
// dispatch, timers, contexts, observer calls, logging -- rather than a store
// implementation. memstore is not used here because its Claim scans every row,
// which is fine for tests but would dominate the measurement.
type fastStore struct {
	msgs []outbox.Claimed
	next atomic.Int64
}

func newFastStore(n int, body []byte) *fastStore {
	s := &fastStore{msgs: make([]outbox.Claimed, n)}
	now := time.Now()
	for i := range s.msgs {
		s.msgs[i] = outbox.Claimed{
			Message: outbox.Message{
				ID:          outbox.NewID(),
				Type:        "orders.created.v1",
				Payload:     body,
				ContentType: "application/json",
				AvailableAt: now,
			},
			Seq:            int64(i),
			Attempt:        1,
			LeaseEpoch:     1,
			LeaseExpiresAt: now.Add(time.Minute),
		}
	}
	return s
}

func (s *fastStore) Claim(_ context.Context, req outbox.ClaimRequest) (outbox.ClaimResult, error) {
	start := s.next.Add(int64(req.Limit)) - int64(req.Limit)
	if start >= int64(len(s.msgs)) {
		return outbox.ClaimResult{}, nil
	}
	end := min(start+int64(req.Limit), int64(len(s.msgs)))
	return outbox.ClaimResult{Messages: s.msgs[start:end]}, nil
}

func (s *fastStore) MarkPublished(context.Context, outbox.ID, int64) error { return nil }
func (s *fastStore) Reschedule(context.Context, outbox.ID, int64, time.Duration, outbox.Failure) error {
	return nil
}
func (s *fastStore) Fail(context.Context, outbox.ID, int64, outbox.Failure) error { return nil }
func (s *fastStore) Release(context.Context, outbox.ID, int64) error              { return nil }
func (s *fastStore) Stats(context.Context) (outbox.Stats, error)                  { return outbox.Stats{}, nil }

// BenchmarkWorkerPipeline measures the library's own overhead per message:
// claim dispatch, per-message context and timeout, observer calls and the
// acknowledgement call itself. The store and publisher do nothing, so the
// result is the ceiling the Postgres numbers are measured against, never a
// production figure.
func BenchmarkWorkerPipeline(b *testing.B) {
	for _, c := range []struct{ concurrency, batch int }{{1, 16}, {4, 32}, {16, 64}} {
		b.Run(fmt.Sprintf("workers=%d/batch=%d", c.concurrency, c.batch), func(b *testing.B) {
			total := b.N
			store := newFastStore(total, benchPayload())
			var done atomic.Int64
			w, err := outbox.NewWorker(store, outbox.PublisherFunc(func(context.Context, outbox.Message) error {
				done.Add(1)
				return nil
			}), outbox.Config{
				Concurrency:   c.concurrency,
				BatchSize:     c.batch,
				PollInterval:  time.Millisecond,
				LeaseDuration: time.Minute,
				StatsInterval: -1,
				Logger:        slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			if err := w.Start(context.Background()); err != nil {
				b.Fatal(err)
			}
			deadline := time.Now().Add(time.Minute)
			for done.Load() < int64(total) && time.Now().Before(deadline) {
				time.Sleep(50 * time.Microsecond)
			}
			b.StopTimer()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = w.Shutdown(ctx)
			cancel()
			if got := done.Load(); got < int64(total) {
				b.Fatalf("published %d of %d", got, total)
			}
			b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "msg/s")
		})
	}
}

// BenchmarkPublishFailurePath measures the retry path, which does more work
// per message than success: classify, compute a delay, write a summary.
func BenchmarkPublishFailurePath(b *testing.B) {
	total := b.N
	store := newFastStore(total, benchPayload())
	var done atomic.Int64
	w, err := outbox.NewWorker(store, outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		done.Add(1)
		return outbox.Permanent(errors.New("broker rejected the message"))
	}), outbox.Config{
		Concurrency:   8,
		BatchSize:     64,
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		StatsInterval: -1,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1})),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	if err := w.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	for done.Load() < int64(total) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Microsecond)
	}
	b.StopTimer()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = w.Shutdown(ctx)
	cancel()
	b.ReportMetric(float64(done.Load())/b.Elapsed().Seconds(), "msg/s")
}
