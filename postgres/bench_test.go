package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tahaNGU/gocast"
	"github.com/tahaNGU/gocast/postgres"
)

// payload returns a realistic event body: a few hundred bytes of JSON, which
// is what a typical domain event serialises to. Microbenchmarks on empty
// payloads flatter the library and tell you nothing about production.
func payload(n int) []byte {
	var b strings.Builder
	b.WriteString(`{"order_id":"01924f1e-2b3c-7d4e-8f90-a1b2c3d4e5f6","customer_id":"cust-99812","status":"CREATED","total":{"amount":12999,"currency":"EUR"},"items":[`)
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"sku":"SKU-%06d","qty":%d,"price":%d}`, i, i%5+1, 199*(i+1))
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

func benchMessages(n, items int) []outbox.Message {
	body := payload(items)
	msgs := make([]outbox.Message, n)
	for i := range msgs {
		msgs[i] = outbox.Message{
			ID:            outbox.NewID(),
			Type:          "orders.created.v1",
			Payload:       body,
			ContentType:   "application/json",
			AggregateType: "order",
			AggregateID:   fmt.Sprintf("order-%d", i),
			Headers:       map[string]string{"tenant": "acme", "region": "eu-central-1"},
		}
		msgs[i].ID = outbox.NewID()
	}
	return msgs
}

func benchStore(b *testing.B, opts ...postgres.Option) *postgres.Store {
	b.Helper()
	if sharedDB == nil {
		b.Skipf("%s is not set", DSNEnv)
	}
	table := fmt.Sprintf("outbox_bench_%d", time.Now().UnixNano()%1_000_000_000)
	opts = append([]postgres.Option{postgres.WithTable(table)}, opts...)
	store, err := postgres.New(sharedDB, opts...)
	if err != nil {
		b.Fatalf("postgres.New: %v", err)
	}
	ctx := context.Background()
	if err := store.Migrate(ctx); err != nil {
		b.Fatalf("Migrate: %v", err)
	}
	b.Cleanup(func() {
		if _, err := sharedDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table); err != nil {
			b.Logf("drop %s: %v", table, err)
		}
	})
	return store
}

// BenchmarkEnqueue measures the cost of adding messages to a business
// transaction, which is the part of the outbox that sits on the request path.
func BenchmarkEnqueue(b *testing.B) {
	for _, batch := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			store := benchStore(b)
			ctx := context.Background()
			msgs := benchMessages(batch, 3)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				for j := range msgs {
					msgs[j].ID = outbox.NewID()
				}
				tx, err := sharedDB.BeginTx(ctx, nil)
				if err != nil {
					b.Fatal(err)
				}
				if err := store.Enqueue(ctx, tx, msgs...); err != nil {
					tx.Rollback()
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(batch)*float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		})
	}
}

// BenchmarkEnqueueStatements isolates serialisation and statement building
// from the database round trip.
func BenchmarkEnqueueStatements(b *testing.B) {
	store, err := postgres.New(nopDB{})
	if err != nil {
		b.Fatal(err)
	}
	for _, batch := range []int{1, 100} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			msgs := benchMessages(batch, 3)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := store.EnqueueStatements(msgs...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkClaim measures the claim query against a table that looks like a
// production one: a large published history plus a modest live backlog.
func BenchmarkClaim(b *testing.B) {
	for _, size := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprintf("limit=%d", size), func(b *testing.B) {
			store := benchStore(b)
			ctx := context.Background()
			seedPublished(b, store, 50_000)
			b.ResetTimer()
			var claimed int
			for b.Loop() {
				b.StopTimer()
				seedPending(b, store, size)
				b.StartTimer()
				res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "bench", Limit: size, Lease: time.Minute})
				if err != nil {
					b.Fatal(err)
				}
				claimed += len(res.Messages)
				b.StopTimer()
				if _, err := sharedDB.ExecContext(ctx, `DELETE FROM `+store.Table()+` WHERE state = 'leased'`); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(claimed)/b.Elapsed().Seconds(), "msg/s")
		})
	}
}

// BenchmarkAcknowledge measures the per-message bookkeeping write, which is
// the dominant database cost of publishing.
func BenchmarkAcknowledge(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		seedPending(b, store, 1)
		res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "bench", Limit: 1, Lease: time.Minute})
		if err != nil || len(res.Messages) != 1 {
			b.Fatalf("claim: %v (%d messages)", err, len(res.Messages))
		}
		m := res.Messages[0]
		b.StartTimer()
		if err := store.MarkPublished(ctx, m.ID, m.LeaseEpoch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEndToEnd measures sustained throughput through a running worker:
// claim, publish and acknowledge, at several worker counts and batch sizes.
// The publisher is a no-op, so the number is an upper bound set by the
// database and the library, not by any broker.
func BenchmarkEndToEnd(b *testing.B) {
	for _, c := range []struct{ concurrency, batch int }{
		{1, 16}, {4, 16}, {8, 32}, {16, 64}, {32, 64},
	} {
		b.Run(fmt.Sprintf("workers=%d/batch=%d", c.concurrency, c.batch), func(b *testing.B) {
			store := benchStore(b)
			ctx := context.Background()
			total := b.N
			seedPending(b, store, total)

			var done atomic.Int64
			cfg := outbox.Config{
				Concurrency:    c.concurrency,
				BatchSize:      c.batch,
				PollInterval:   time.Millisecond,
				LeaseDuration:  time.Minute,
				PublishTimeout: 10 * time.Second,
				StoreTimeout:   10 * time.Second,
				StatsInterval:  -1,
				Logger:         slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
			}
			w, err := outbox.NewWorker(store, outbox.PublisherFunc(func(context.Context, outbox.Message) error {
				done.Add(1)
				return nil
			}), cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			if err := w.Start(ctx); err != nil {
				b.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Minute)
			for done.Load() < int64(total) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			b.StopTimer()
			shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			_ = w.Shutdown(shutdownCtx)
			cancel()
			if got := done.Load(); got < int64(total) {
				b.Fatalf("published %d of %d messages before the deadline", got, total)
			}
			b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "msg/s")
		})
	}
}

// BenchmarkStats measures the sampling query against a large table, since it
// is the one query whose cost grows with the backlog.
func BenchmarkStats(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()
	seedPublished(b, store, 50_000)
	seedPending(b, store, 5_000)
	b.ResetTimer()
	for b.Loop() {
		if _, err := store.Stats(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func seedPending(tb testing.TB, store *postgres.Store, n int) {
	tb.Helper()
	ctx := context.Background()
	msgs := benchMessages(n, 3)
	tx, err := sharedDB.BeginTx(ctx, nil)
	if err != nil {
		tb.Fatal(err)
	}
	if err := store.Enqueue(ctx, tx, msgs...); err != nil {
		tx.Rollback()
		tb.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

// seedPublished fills the table with published history, the way a production
// table looks after a few days.
func seedPublished(tb testing.TB, store *postgres.Store, n int) {
	tb.Helper()
	_, err := sharedDB.ExecContext(context.Background(), `
		INSERT INTO `+store.Table()+` (id, event_type, payload, state, published_at, available_at, attempts)
		SELECT gen_random_uuid(), 'orders.created.v1', $1, 'published',
		       now() - (g || ' seconds')::interval, now() - (g || ' seconds')::interval, 1
		  FROM generate_series(1, $2) g`, payload(3), n)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := sharedDB.ExecContext(context.Background(), `ANALYZE `+store.Table()); err != nil {
		tb.Fatal(err)
	}
}

// nopDB satisfies postgres.DB for benchmarks that never touch a database.
type nopDB struct{}

func (nopDB) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, io.EOF
}
func (nopDB) QueryContext(context.Context, string, ...any) (*sql.Rows, error) { return nil, io.EOF }
func (nopDB) QueryRowContext(context.Context, string, ...any) *sql.Row        { return nil }
