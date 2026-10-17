// Package pgxcompat holds a compatibility test only.
package pgxcompat_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/tahaNGU/gocast"
	"github.com/tahaNGU/gocast/postgres"
)

// TestStoreWorksWithPgxStdlib verifies the claim that the store works with any database/sql driver
// by exercising the full state machine through pgx/stdlib rather than lib/pq.
func TestStoreWorksWithPgxStdlib(t *testing.T) {
	dsn := os.Getenv("OUTBOX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_POSTGRES_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	store, err := postgres.New(db, postgres.WithTable("outbox_pgxcompat"), postgres.WithOrdering())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS outbox_pgxcompat"); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	defer db.ExecContext(context.Background(), "DROP TABLE IF EXISTS outbox_pgxcompat")

	// Enqueue in a transaction, with every column type exercised.
	id := outbox.NewID()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Enqueue(ctx, tx, outbox.Message{
		ID:            id,
		Type:          "orders.created.v1",
		Payload:       []byte{0x00, 0xff, 0x01},
		ContentType:   "application/octet-stream",
		AggregateType: "order",
		AggregateID:   "o-1",
		OrderingKey:   "o-1",
		Headers:       map[string]string{"tenant": "acme"},
		AvailableAt:   time.Now().Add(-time.Minute),
	})
	if err != nil {
		tx.Rollback()
		t.Fatalf("enqueue: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "pgx", Limit: 10, Lease: time.Minute})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(res.Messages) != 1 {
		t.Fatalf("claimed %d messages, want 1", len(res.Messages))
	}
	m := res.Messages[0]
	if m.ID != id || m.Type != "orders.created.v1" || string(m.Payload) != string([]byte{0x00, 0xff, 0x01}) ||
		m.Headers["tenant"] != "acme" || m.OrderingKey != "o-1" || m.Attempt != 1 || m.DecodeErr != nil {
		t.Fatalf("round trip through pgx lost data: %+v (decode err %v)", m, m.DecodeErr)
	}
	if _, err := store.Stats(ctx); err != nil {
		t.Fatalf("stats: %v", err)
	}
	if err := store.Reschedule(ctx, m.ID, m.LeaseEpoch, time.Millisecond, outbox.Failure{Kind: "retryable", Error: "x"}); err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	res, err = store.Claim(ctx, outbox.ClaimRequest{Owner: "pgx", Limit: 10, Lease: time.Minute})
	if err != nil || len(res.Messages) != 1 {
		t.Fatalf("reclaim after reschedule: %+v %v", res, err)
	}
	m = res.Messages[0]
	if err := store.MarkPublished(ctx, m.ID, m.LeaseEpoch); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if n, err := store.DeletePublished(ctx, time.Nanosecond, 10); err != nil || n != 1 {
		t.Fatalf("delete published = %d, %v", n, err)
	}
}
