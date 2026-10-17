package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tahaNGU/gocast"
	"github.com/tahaNGU/gocast/postgres"
)

// ---------- construction ----------

func TestNewRejectsUnsafeTableNames(t *testing.T) {
	for _, name := range []string{
		"", "outbox messages", "outbox;DROP TABLE users", `"outbox"`,
		"a.b.c", "1outbox", "outbox-messages", "outbox'",
	} {
		if _, err := postgres.New(sharedDB, postgres.WithTable(name)); err == nil {
			t.Errorf("WithTable(%q) was accepted", name)
		}
	}
	for _, name := range []string{"outbox_messages", "app.outbox_messages", "_x1"} {
		if _, err := postgres.New(sharedDB, postgres.WithTable(name)); err != nil {
			t.Errorf("WithTable(%q) was rejected: %v", name, err)
		}
	}
	if _, err := postgres.New(nil); err == nil {
		t.Error("New(nil) was accepted")
	}
	if _, err := postgres.New(sharedDB, postgres.WithMaxEnqueueBatch(0)); err == nil {
		t.Error("WithMaxEnqueueBatch(0) was accepted")
	}
}

func TestMigrationsRenderTableName(t *testing.T) {
	ms, err := postgres.Migrations("app.events_outbox", true)
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	if len(ms) != 2 {
		t.Fatalf("got %d migrations, want 2 (table + ordering index)", len(ms))
	}
	if !contains(ms[0].SQL, "CREATE TABLE IF NOT EXISTS app.events_outbox") {
		t.Errorf("table name not rendered: %s", ms[0].SQL)
	}
	if !contains(ms[0].SQL, "CONSTRAINT events_outbox_state_check") {
		t.Errorf("constraint prefix not rendered: %s", ms[0].SQL)
	}
	if contains(ms[0].SQL, "{{") {
		t.Errorf("unrendered placeholder left behind: %s", ms[0].SQL)
	}
	if !ms[1].Ordering || !contains(ms[1].SQL, "ordering_key") {
		t.Errorf("second migration is not the ordering index: %+v", ms[1])
	}

	plain, err := postgres.Migrations(postgres.DefaultTable, false)
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	if len(plain) != 1 {
		t.Fatalf("got %d migrations without ordering, want 1", len(plain))
	}
	down, err := postgres.DownMigrations(postgres.DefaultTable, true)
	if err != nil {
		t.Fatalf("DownMigrations: %v", err)
	}
	if len(down) != 2 || !down[0].Ordering {
		t.Fatalf("down migrations must be reversed: %+v", down)
	}
	if _, err := postgres.Migrations("bad name", false); err == nil {
		t.Error("Migrations accepted an invalid table name")
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	for range 3 {
		if err := store.Migrate(ctx); err != nil {
			t.Fatalf("re-applying the migration failed: %v", err)
		}
	}
	// The indexes the hot paths rely on must exist.
	rows, err := sharedDB.QueryContext(ctx,
		`SELECT indexname FROM pg_indexes WHERE tablename = $1`, store.Table())
	requireNoError(t, err, "list indexes")
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var name string
		requireNoError(t, rows.Scan(&name), "scan index name")
		found[name] = true
	}
	for _, suffix := range []string{"_pkey", "_due_idx", "_failed_idx", "_published_idx"} {
		if !found[store.Table()+suffix] {
			t.Errorf("index %s%s is missing (have %v)", store.Table(), suffix, found)
		}
	}
}

// ---------- the outbox invariant ----------

func TestEnqueueCommitsWithTheBusinessTransaction(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS outbox_test_accounts (id text PRIMARY KEY, balance bigint NOT NULL)`)
	requireNoError(t, err, "create business table")
	t.Cleanup(func() { db.ExecContext(context.Background(), `DROP TABLE IF EXISTS outbox_test_accounts`) })
	_, err = db.ExecContext(ctx, `TRUNCATE outbox_test_accounts`)
	requireNoError(t, err, "truncate")

	id := outbox.NewID()
	tx, err := db.BeginTx(ctx, nil)
	requireNoError(t, err, "begin")
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_test_accounts (id, balance) VALUES ('a', 100)`)
	requireNoError(t, err, "business write")
	requireNoError(t, store.Enqueue(ctx, tx, outbox.Message{ID: id, Type: "account.created.v1"}), "enqueue")

	// Nothing is visible to other transactions before the commit.
	if n := countState(t, store, outbox.StatePending); n != 0 {
		t.Fatalf("%d outbox rows visible before commit, want 0", n)
	}
	requireNoError(t, tx.Commit(), "commit")

	if n := countState(t, store, outbox.StatePending); n != 1 {
		t.Fatalf("%d outbox rows after commit, want 1", n)
	}
	var balance int64
	requireNoError(t, db.QueryRowContext(ctx, `SELECT balance FROM outbox_test_accounts WHERE id = 'a'`).Scan(&balance), "read business row")
	if balance != 100 {
		t.Fatalf("balance = %d, want 100", balance)
	}
}

func TestRollbackDiscardsBusinessChangeAndOutboxRow(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS outbox_test_accounts2 (id text PRIMARY KEY)`)
	requireNoError(t, err, "create business table")
	t.Cleanup(func() { db.ExecContext(context.Background(), `DROP TABLE IF EXISTS outbox_test_accounts2`) })

	tx, err := db.BeginTx(ctx, nil)
	requireNoError(t, err, "begin")
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_test_accounts2 (id) VALUES ('b')`)
	requireNoError(t, err, "business write")
	requireNoError(t, store.Enqueue(ctx, tx, outbox.Message{Type: "account.created.v1"}), "enqueue")
	requireNoError(t, tx.Rollback(), "rollback")

	if n := countState(t, store, outbox.StatePending); n != 0 {
		t.Fatalf("%d outbox rows survived the rollback, want 0", n)
	}
	var n int
	requireNoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM outbox_test_accounts2`).Scan(&n), "count business rows")
	if n != 0 {
		t.Fatalf("%d business rows survived the rollback, want 0", n)
	}
}

func TestEnqueueFailureRollsBackTheWholeBatch(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	id := outbox.NewID()
	tx, err := db.BeginTx(ctx, nil)
	requireNoError(t, err, "begin")
	defer tx.Rollback()
	// A duplicate id inside one batch must fail the statement, and with it the
	// whole enqueue.
	err = store.Enqueue(ctx, tx,
		outbox.Message{ID: id, Type: "t"},
		outbox.Message{ID: id, Type: "t"})
	if err == nil {
		t.Fatal("duplicate ids in one batch were accepted")
	}
	if !contains(err.Error(), "insert into") {
		t.Errorf("error does not say what failed: %v", err)
	}
}

func TestEnqueueStoresEveryFieldFaithfully(t *testing.T) {
	store, _ := newStore(t)
	id := outbox.NewID()
	future := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	enqueueCommitted(t, store, outbox.Message{
		ID:            id,
		Type:          "orders.created.v1",
		Payload:       []byte{0x00, 0x01, 0xff, 0xfe}, // binary-safe
		ContentType:   "application/protobuf",
		AggregateType: "order",
		AggregateID:   "o-1",
		OrderingKey:   "o-1",
		Headers:       map[string]string{"tenant": "acme", "x": "y"},
		AvailableAt:   future,
	})
	r := readRow(t, store, id)
	if r.State != string(outbox.StatePending) || r.Attempts != 0 || r.LeaseEpoch != 0 {
		t.Errorf("fresh row = %+v, want pending with no attempts and epoch 0", r)
	}
	if string(r.Payload) != string([]byte{0x00, 0x01, 0xff, 0xfe}) {
		t.Errorf("payload round trip = %v", r.Payload)
	}
	if !r.AvailableAt.UTC().Equal(future) {
		t.Errorf("available_at = %s, want %s", r.AvailableAt.UTC(), future)
	}
	if r.OrderingKey.String != "o-1" {
		t.Errorf("ordering_key = %+v", r.OrderingKey)
	}
	if !contains(r.Headers, `"tenant": "acme"`) && !contains(r.Headers, `"tenant":"acme"`) {
		t.Errorf("headers = %s", r.Headers)
	}
	if r.CreatedAt.IsZero() {
		t.Error("created_at was not set by the database")
	}
	if r.LeaseOwner.Valid || r.PublishedAt.Valid {
		t.Errorf("fresh row already has a lease owner or publication time: %+v", r)
	}

	// An empty ordering key must be NULL so the partial index stays small.
	id2 := outbox.NewID()
	enqueueCommitted(t, store, outbox.Message{ID: id2, Type: "t"})
	if r := readRow(t, store, id2); r.OrderingKey.Valid {
		t.Errorf("empty ordering key stored as %q, want NULL", r.OrderingKey.String)
	}
	if r := readRow(t, store, id2); len(r.Payload) != 0 {
		t.Errorf("nil payload stored as %v, want empty", r.Payload)
	}
}

func TestEnqueueRejectsInvalidMessagesBeforeTouchingTheDatabase(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	requireNoError(t, err, "begin")
	defer tx.Rollback()
	err = store.Enqueue(ctx, tx, outbox.Message{Type: "ok"}, outbox.Message{ /* no type */ })
	if !errors.Is(err, outbox.ErrInvalidMessage) {
		t.Fatalf("error = %v, want ErrInvalidMessage", err)
	}
	if err := store.Enqueue(ctx, nil, outbox.Message{Type: "t"}); err == nil {
		t.Error("enqueue with a nil transaction was accepted")
	}
	if err := store.Enqueue(ctx, tx); err != nil {
		t.Errorf("enqueue of nothing = %v, want nil", err)
	}
}

func TestEnqueueSplitsLargeBatchesAtomically(t *testing.T) {
	store, db := newStore(t, postgres.WithMaxEnqueueBatch(7))
	ctx := context.Background()
	msgs := make([]outbox.Message, 50)
	for i := range msgs {
		msgs[i] = outbox.Message{ID: outbox.NewID(), Type: "t", Payload: []byte(fmt.Sprint(i))}
	}
	stmts, err := store.EnqueueStatements(msgs...)
	requireNoError(t, err, "EnqueueStatements")
	if len(stmts) != 8 {
		t.Fatalf("got %d statements for 50 messages at batch 7, want 8", len(stmts))
	}

	tx, err := db.BeginTx(ctx, nil)
	requireNoError(t, err, "begin")
	requireNoError(t, store.Enqueue(ctx, tx, msgs...), "enqueue")
	requireNoError(t, tx.Rollback(), "rollback")
	if n := countState(t, store, outbox.StatePending); n != 0 {
		t.Fatalf("%d of the split batch survived a rollback, want 0", n)
	}

	tx, err = db.BeginTx(ctx, nil)
	requireNoError(t, err, "begin")
	requireNoError(t, store.Enqueue(ctx, tx, msgs...), "enqueue")
	requireNoError(t, tx.Commit(), "commit")
	if n := countState(t, store, outbox.StatePending); n != 50 {
		t.Fatalf("stored %d messages, want 50", n)
	}
}

// ---------- claiming ----------

func TestClaimLeasesAndFences(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"})

	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w1", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "claim")
	if len(res.Messages) != 1 {
		t.Fatalf("claimed %d messages, want 1", len(res.Messages))
	}
	m := res.Messages[0]
	if m.Attempt != 1 || m.LeaseEpoch != 1 {
		t.Errorf("claimed message = attempt %d, epoch %d; want 1, 1", m.Attempt, m.LeaseEpoch)
	}
	if m.ReclaimedLease || res.ExpiredLeases != 0 {
		t.Error("a fresh claim must not look like a reclaim")
	}
	if m.LeaseExpiresAt.Before(time.Now()) {
		t.Errorf("lease already expired at %s", m.LeaseExpiresAt)
	}
	r := readRow(t, store, ids[0])
	if r.State != string(outbox.StateLeased) || r.LeaseOwner.String != "w1" {
		t.Errorf("stored row = %+v, want leased by w1", r)
	}
	// For a leased row, available_at is the lease expiry.
	if !r.AvailableAt.UTC().Equal(m.LeaseExpiresAt) {
		t.Errorf("available_at = %s, want the lease expiry %s", r.AvailableAt.UTC(), m.LeaseExpiresAt)
	}

	// A leased message is not claimable again until its lease expires.
	again, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w2", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "second claim")
	if len(again.Messages) != 0 {
		t.Fatalf("claimed %d messages while leased, want 0", len(again.Messages))
	}

	requireNoError(t, store.MarkPublished(ctx, m.ID, m.LeaseEpoch), "mark published")
	r = readRow(t, store, ids[0])
	if r.State != string(outbox.StatePublished) || !r.PublishedAt.Valid {
		t.Errorf("published row = %+v, want published", r)
	}
	if r.LeaseOwner.Valid {
		t.Errorf("lease owner = %+v, want NULL after publication", r.LeaseOwner)
	}
}

func TestClaimSkipsMessagesScheduledForTheFuture(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	enqueueCommitted(t, store,
		outbox.Message{Type: "later", AvailableAt: time.Now().Add(time.Hour)},
		outbox.Message{Type: "now"},
	)
	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "claim")
	if len(res.Messages) != 1 || res.Messages[0].Type != "now" {
		t.Fatalf("claimed %+v, want only the due message", res.Messages)
	}
}

func TestClaimReclaimsExpiredLeasesAndAdvancesTheEpoch(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"})

	first, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "a", Limit: 1, Lease: 300 * time.Millisecond})
	requireNoError(t, err, "first claim")
	time.Sleep(400 * time.Millisecond)

	second, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "b", Limit: 1, Lease: time.Minute})
	requireNoError(t, err, "reclaim")
	if len(second.Messages) != 1 {
		t.Fatalf("reclaimed %d messages, want 1", len(second.Messages))
	}
	if second.ExpiredLeases != 1 || !second.Messages[0].ReclaimedLease {
		t.Error("an expired lease must be reported as reclaimed")
	}
	if second.Messages[0].Attempt != 2 {
		t.Errorf("attempt = %d, want 2: a crashed attempt must still count", second.Messages[0].Attempt)
	}
	if second.Messages[0].LeaseEpoch != first.Messages[0].LeaseEpoch+1 {
		t.Errorf("epoch = %d, want %d", second.Messages[0].LeaseEpoch, first.Messages[0].LeaseEpoch+1)
	}

	// Worker A is now stale: every update it attempts must be rejected.
	stale := first.Messages[0]
	for name, err := range map[string]error{
		"mark published": store.MarkPublished(ctx, stale.ID, stale.LeaseEpoch),
		"reschedule":     store.Reschedule(ctx, stale.ID, stale.LeaseEpoch, time.Minute, outbox.Failure{Kind: "retryable", Error: "x"}),
		"fail":           store.Fail(ctx, stale.ID, stale.LeaseEpoch, outbox.Failure{Kind: "permanent", Error: "x"}),
		"release":        store.Release(ctx, stale.ID, stale.LeaseEpoch),
	} {
		if !errors.Is(err, outbox.ErrLeaseLost) {
			t.Errorf("stale %s = %v, want ErrLeaseLost", name, err)
		}
	}
	if r := readRow(t, store, ids[0]); r.State != string(outbox.StateLeased) || r.LeaseOwner.String != "b" {
		t.Fatalf("row = %+v, want still leased by b", r)
	}
}

func TestFencedUpdatesReportMissingMessages(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	missing := outbox.NewID()
	err := store.MarkPublished(ctx, missing, 1)
	if !errors.Is(err, outbox.ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
	if !contains(err.Error(), missing.String()) {
		t.Errorf("error does not name the message: %v", err)
	}
}

func TestConcurrentWorkersNeverClaimTheSameMessage(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	const total = 400
	msgs := make([]outbox.Message, total)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t", Payload: []byte(fmt.Sprint(i))}
	}
	enqueueCommitted(t, store, msgs...)

	var (
		mu    sync.Mutex
		owner = make(map[outbox.ID]string, total)
		wg    sync.WaitGroup
	)
	for w := range 8 {
		name := fmt.Sprintf("w%d", w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: name, Limit: 13, Lease: 5 * time.Minute})
				if err != nil {
					t.Errorf("%s: claim: %v", name, err)
					return
				}
				if len(res.Messages) == 0 {
					return
				}
				mu.Lock()
				for _, m := range res.Messages {
					if prev, dup := owner[m.ID]; dup {
						t.Errorf("%s was claimed by both %s and %s", m.ID, prev, name)
					}
					owner[m.ID] = name
					if m.Attempt != 1 {
						t.Errorf("%s: attempt = %d, want 1", m.ID, m.Attempt)
					}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(owner) != total {
		t.Fatalf("claimed %d distinct messages, want %d", len(owner), total)
	}
	if n := countState(t, store, outbox.StateLeased); n != total {
		t.Fatalf("%d leased rows, want %d", n, total)
	}
}

func TestClaimIsBoundedByLimit(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	msgs := make([]outbox.Message, 20)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t"}
	}
	enqueueCommitted(t, store, msgs...)
	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 5, Lease: time.Minute})
	requireNoError(t, err, "claim")
	if len(res.Messages) != 5 {
		t.Fatalf("claimed %d messages, want 5", len(res.Messages))
	}
	for _, bad := range []outbox.ClaimRequest{
		{Owner: "w", Limit: 0, Lease: time.Minute},
		{Owner: "w", Limit: 5, Lease: 0},
	} {
		if _, err := store.Claim(ctx, bad); err == nil {
			t.Errorf("Claim accepted %+v", bad)
		}
	}
}

func TestClaimReturnsOldestDueFirst(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store,
		outbox.Message{Type: "first"},
		outbox.Message{Type: "second"},
		outbox.Message{Type: "third"},
	)
	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 3, Lease: time.Minute})
	requireNoError(t, err, "claim")
	if len(res.Messages) != 3 {
		t.Fatalf("claimed %d, want 3", len(res.Messages))
	}
	for i, m := range res.Messages {
		if m.ID != ids[i] {
			t.Errorf("position %d = %s, want %s (enqueue order)", i, m.ID, ids[i])
		}
		if m.Seq == 0 {
			t.Errorf("%s has no sequence number", m.ID)
		}
	}
}

// ---------- retries and failures ----------

func TestRescheduleStoresTheScheduleAndSummary(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"})
	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Minute})
	requireNoError(t, err, "claim")
	m := res.Messages[0]

	requireNoError(t, store.Reschedule(ctx, m.ID, m.LeaseEpoch, 2*time.Hour,
		outbox.Failure{Kind: "retryable", Error: "broker refused the connection"}), "reschedule")

	r := readRow(t, store, ids[0])
	if r.State != string(outbox.StatePending) {
		t.Errorf("state = %s, want pending", r.State)
	}
	if r.Attempts != 1 {
		t.Errorf("attempts = %d, want the claim-time count to stand", r.Attempts)
	}
	if r.LeaseOwner.Valid {
		t.Errorf("lease not cleared: %+v", r)
	}
	if delay := time.Until(r.AvailableAt); delay < 90*time.Minute {
		t.Errorf("available_at is %s away, want about 2h", delay)
	}
	if r.ErrorKind.String != "retryable" || !contains(r.LastError.String, "broker refused") {
		t.Errorf("failure summary = %+v / %+v", r.ErrorKind, r.LastError)
	}

	// The schedule is in the database, so it survives any restart: the message
	// is not claimable until it is due.
	empty, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "claim")
	if len(empty.Messages) != 0 {
		t.Fatalf("claimed %d messages before the retry was due", len(empty.Messages))
	}
}

func TestFailIsTerminalAndRecoverable(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store, outbox.Message{Type: "orders.created.v1", AggregateType: "order", AggregateID: "o-9"})
	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Minute})
	requireNoError(t, err, "claim")
	m := res.Messages[0]
	requireNoError(t, store.Fail(ctx, m.ID, m.LeaseEpoch,
		outbox.Failure{Kind: string(outbox.ReasonPermanent), Error: "topic does not exist"}), "fail")

	if r := readRow(t, store, ids[0]); r.State != string(outbox.StateFailed) || r.LeaseOwner.Valid {
		t.Fatalf("row = %+v, want failed with no lease", r)
	}
	// A failed message is never claimed again on its own.
	empty, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "claim")
	if len(empty.Messages) != 0 {
		t.Fatalf("a failed message was claimed again: %+v", empty.Messages)
	}

	listed, err := store.ListFailed(ctx, 10, outbox.ID{})
	requireNoError(t, err, "list failed")
	if len(listed) != 1 {
		t.Fatalf("listed %d failed messages, want 1", len(listed))
	}
	got := listed[0]
	if got.ID != ids[0] || got.Type != "orders.created.v1" || got.Attempts != 1 ||
		got.ErrorKind != string(outbox.ReasonPermanent) || !contains(got.LastError, "topic does not exist") ||
		got.Aggregate != "order/o-9" || got.FailedAt.IsZero() {
		t.Fatalf("listed message = %+v", got)
	}

	n, err := store.Requeue(ctx, ids[0])
	requireNoError(t, err, "requeue")
	if n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	r := readRow(t, store, ids[0])
	if r.State != string(outbox.StatePending) || r.Attempts != 0 || r.LastError.Valid {
		t.Fatalf("requeued row = %+v, want a clean pending row", r)
	}
	again, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "claim")
	if len(again.Messages) != 1 {
		t.Fatalf("a requeued message was not claimable: %+v", again)
	}
}

func TestRequeueOnlyTouchesFailedMessages(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store, outbox.Message{Type: "pending"}, outbox.Message{Type: "published"})

	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "claim")
	for _, m := range res.Messages {
		if m.Type == "published" {
			requireNoError(t, store.MarkPublished(ctx, m.ID, m.LeaseEpoch), "mark published")
		} else {
			requireNoError(t, store.Release(ctx, m.ID, m.LeaseEpoch), "release")
		}
	}
	n, err := store.Requeue(ctx, ids...)
	requireNoError(t, err, "requeue")
	if n != 0 {
		t.Fatalf("requeued %d non-failed messages, want 0", n)
	}
	if r := readRow(t, store, ids[1]); r.State != string(outbox.StatePublished) {
		t.Fatalf("a published message was changed by requeue: %+v", r)
	}
	if got, err := store.Requeue(ctx); err != nil || got != 0 {
		t.Fatalf("Requeue() with no ids = %d, %v", got, err)
	}
}

func TestListFailedPaginates(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	msgs := make([]outbox.Message, 5)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: fmt.Sprintf("t%d", i)}
	}
	ids := enqueueCommitted(t, store, msgs...)
	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 5, Lease: time.Minute})
	requireNoError(t, err, "claim")
	for _, m := range res.Messages {
		requireNoError(t, store.Fail(ctx, m.ID, m.LeaseEpoch, outbox.Failure{Kind: "permanent", Error: "x"}), "fail")
	}

	page1, err := store.ListFailed(ctx, 2, outbox.ID{})
	requireNoError(t, err, "page 1")
	if len(page1) != 2 || page1[0].ID != ids[0] {
		t.Fatalf("page 1 = %+v", page1)
	}
	page2, err := store.ListFailed(ctx, 2, page1[1].ID)
	requireNoError(t, err, "page 2")
	if len(page2) != 2 || page2[0].ID != ids[2] {
		t.Fatalf("page 2 = %+v", page2)
	}
	if _, err := store.ListFailed(ctx, 0, outbox.ID{}); err == nil {
		t.Error("ListFailed accepted a zero limit")
	}
}

// ---------- malformed data ----------

func TestClaimReportsUndecodableHeadersInsteadOfFailing(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"}, outbox.Message{Type: "healthy"})
	// jsonb cannot hold a non-object, so corrupt the row the way a bad
	// migration or a direct write would: a JSON scalar where a map is expected.
	_, err := db.ExecContext(ctx, `UPDATE `+store.Table()+` SET headers = '"not-a-map"'::jsonb WHERE id = $1`, ids[0])
	requireNoError(t, err, "corrupt headers")

	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "claim must not fail because one row is unreadable")
	if len(res.Messages) != 2 {
		t.Fatalf("claimed %d messages, want 2", len(res.Messages))
	}
	var bad, good int
	for _, m := range res.Messages {
		if m.ID == ids[0] {
			bad++
			if m.DecodeErr == nil {
				t.Error("the corrupt row was returned without a decode error")
			}
			if m.Headers != nil {
				t.Error("the corrupt row returned headers anyway")
			}
		} else {
			good++
			if m.DecodeErr != nil {
				t.Errorf("a healthy row reported %v", m.DecodeErr)
			}
		}
	}
	if bad != 1 || good != 1 {
		t.Fatalf("got %d corrupt and %d healthy rows", bad, good)
	}
}

// ---------- stats, retention ----------

func TestStatsReportsDepthAndLag(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	enqueueCommitted(t, store,
		outbox.Message{Type: "a"},
		outbox.Message{Type: "b"},
		outbox.Message{Type: "c", AvailableAt: time.Now().Add(time.Hour)},
	)
	time.Sleep(50 * time.Millisecond)

	st, err := store.Stats(ctx)
	requireNoError(t, err, "stats")
	if st.Pending != 3 || st.Leased != 0 || st.Failed != 0 {
		t.Fatalf("stats = %+v, want 3 pending", st)
	}
	if st.OldestPendingAge <= 0 {
		t.Errorf("oldest pending age = %s, want a positive lag", st.OldestPendingAge)
	}
	if st.ObservedAt.IsZero() || st.ObservedAt.Location() != time.UTC {
		t.Errorf("observed at = %s, want a UTC timestamp", st.ObservedAt)
	}

	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Minute})
	requireNoError(t, err, "claim")
	requireNoError(t, store.Fail(ctx, res.Messages[0].ID, res.Messages[0].LeaseEpoch,
		outbox.Failure{Kind: "permanent"}), "fail")
	res, err = store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Minute})
	requireNoError(t, err, "claim")

	st, err = store.Stats(ctx)
	requireNoError(t, err, "stats")
	if st.Pending != 1 || st.Leased != 1 || st.Failed != 1 {
		t.Fatalf("stats = %+v, want 1 pending, 1 leased, 1 failed", st)
	}
	// Only the future-dated message is pending now, so there is no lag.
	if st.OldestPendingAge != 0 {
		t.Errorf("oldest pending age = %s, want 0 when nothing is due", st.OldestPendingAge)
	}
}

func TestDeletePublishedNeverTouchesUndeliveredMessages(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store,
		outbox.Message{Type: "published"},
		outbox.Message{Type: "pending"},
		outbox.Message{Type: "failed"},
	)
	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "claim")
	for _, m := range res.Messages {
		switch m.Type {
		case "published":
			requireNoError(t, store.MarkPublished(ctx, m.ID, m.LeaseEpoch), "mark published")
		case "failed":
			requireNoError(t, store.Fail(ctx, m.ID, m.LeaseEpoch, outbox.Failure{Kind: "permanent"}), "fail")
		default:
			requireNoError(t, store.Release(ctx, m.ID, m.LeaseEpoch), "release")
		}
	}
	// Age the published row.
	_, err = db.ExecContext(ctx, `UPDATE `+store.Table()+` SET published_at = now() - interval '2 hours' WHERE id = $1`, ids[0])
	requireNoError(t, err, "age the published row")

	n, err := store.DeletePublished(ctx, time.Hour, 100)
	requireNoError(t, err, "delete published")
	if n != 1 {
		t.Fatalf("deleted %d rows, want 1", n)
	}
	for _, id := range ids[1:] {
		if r := readRow(t, store, id); r.State == "" {
			t.Fatalf("%s was deleted", id)
		}
	}
	// A recently published row is kept.
	n, err = store.DeletePublished(ctx, time.Hour, 100)
	requireNoError(t, err, "second sweep")
	if n != 0 {
		t.Fatalf("second sweep deleted %d rows, want 0", n)
	}
	for _, bad := range [][2]any{{time.Duration(0), 10}, {time.Hour, 0}} {
		if _, err := store.DeletePublished(ctx, bad[0].(time.Duration), bad[1].(int)); err == nil {
			t.Errorf("DeletePublished accepted %v", bad)
		}
	}
}

func TestDeletePublishedRespectsTheLimit(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	msgs := make([]outbox.Message, 10)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t"}
	}
	enqueueCommitted(t, store, msgs...)
	res, err := store.Claim(ctx, outbox.ClaimRequest{Owner: "w", Limit: 10, Lease: time.Minute})
	requireNoError(t, err, "claim")
	for _, m := range res.Messages {
		requireNoError(t, store.MarkPublished(ctx, m.ID, m.LeaseEpoch), "mark published")
	}
	_, err = db.ExecContext(ctx, `UPDATE `+store.Table()+` SET published_at = now() - interval '1 day'`)
	requireNoError(t, err, "age rows")

	n, err := store.DeletePublished(ctx, time.Hour, 4)
	requireNoError(t, err, "prune")
	if n != 4 {
		t.Fatalf("deleted %d rows, want the limit of 4", n)
	}
	if left := countState(t, store, outbox.StatePublished); left != 6 {
		t.Fatalf("%d published rows left, want 6", left)
	}
}

// ---------- schema integrity ----------

func TestSchemaRejectsInconsistentRows(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	ids := enqueueCommitted(t, store, outbox.Message{Type: "t"})
	for name, stmt := range map[string]string{
		"unknown state":        `UPDATE ` + store.Table() + ` SET state = 'weird' WHERE id = $1`,
		"negative attempts":    `UPDATE ` + store.Table() + ` SET attempts = -1 WHERE id = $1`,
		"leased without lease": `UPDATE ` + store.Table() + ` SET state = 'leased' WHERE id = $1`,
		"published without ts": `UPDATE ` + store.Table() + ` SET state = 'published' WHERE id = $1`,
	} {
		if _, err := db.ExecContext(ctx, stmt, ids[0]); err == nil {
			t.Errorf("%s: the schema accepted an inconsistent row", name)
		}
	}
	var dup sql.NullString
	if err := db.QueryRowContext(ctx,
		`INSERT INTO `+store.Table()+` (id, event_type, payload) VALUES ($1, 't', '') RETURNING id::text`,
		ids[0]).Scan(&dup); err == nil {
		t.Error("the schema accepted a duplicate id")
	}
}

func TestClaimUsesTheIndexNotASequentialScan(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	// Enough rows that the planner would prefer an index for a small LIMIT.
	msgs := make([]outbox.Message, 2000)
	for i := range msgs {
		msgs[i] = outbox.Message{Type: "t", Payload: []byte(strings.Repeat("x", 64))}
	}
	enqueueCommitted(t, store, msgs...)
	_, err := db.ExecContext(ctx, `ANALYZE `+store.Table())
	requireNoError(t, err, "analyze")

	rows, err := db.QueryContext(ctx, `EXPLAIN (FORMAT TEXT)
		SELECT m.id FROM `+store.Table()+` m
		 WHERE m.state IN ('pending', 'leased') AND m.available_at <= now()
		 ORDER BY m.available_at, m.seq LIMIT 10 FOR UPDATE SKIP LOCKED`)
	requireNoError(t, err, "explain")
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		requireNoError(t, rows.Scan(&line), "scan plan")
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	if contains(plan.String(), "Seq Scan") {
		t.Errorf("the claim query falls back to a sequential scan:\n%s", plan.String())
	}
	// A sort would mean every due row is read before the LIMIT applies, which
	// is what makes a large backlog expensive.
	if contains(plan.String(), "Sort") {
		t.Errorf("the claim query sorts instead of walking the index in order:\n%s", plan.String())
	}
	if !contains(plan.String(), store.Table()+"_due_idx") {
		t.Errorf("the claim query does not use the due index:\n%s", plan.String())
	}
	t.Logf("claim plan:\n%s", plan.String())
}

func TestAvailableAtInThePastIsClampedToNow(t *testing.T) {
	store, _ := newStore(t)
	id := outbox.NewID()
	enqueueCommitted(t, store, outbox.Message{
		ID:          id,
		Type:        "backfill",
		AvailableAt: time.Now().Add(-72 * time.Hour),
	})
	r := readRow(t, store, id)
	if age := time.Since(r.AvailableAt); age > time.Minute {
		t.Fatalf("available_at is %s old; a past value must be clamped to now so a "+
			"backfill does not look like a publishing backlog", age)
	}
	// It is still immediately claimable.
	res, err := store.Claim(context.Background(), outbox.ClaimRequest{Owner: "w", Limit: 1, Lease: time.Minute})
	requireNoError(t, err, "claim")
	if len(res.Messages) != 1 {
		t.Fatalf("claimed %d messages, want 1", len(res.Messages))
	}
	st, err := store.Stats(context.Background())
	requireNoError(t, err, "stats")
	if st.OldestPendingAge > time.Minute {
		t.Errorf("oldest pending age = %s, want a small value", st.OldestPendingAge)
	}
}

func TestEnqueueBatchBoundIsValidated(t *testing.T) {
	for _, n := range []int{0, -1, postgres.MaxEnqueueBatch + 1, 1 << 20} {
		if _, err := postgres.New(sharedDB, postgres.WithMaxEnqueueBatch(n)); err == nil {
			t.Errorf("WithMaxEnqueueBatch(%d) was accepted; PostgreSQL allows 65535 bind parameters", n)
		}
	}
	if _, err := postgres.New(sharedDB, postgres.WithMaxEnqueueBatch(postgres.MaxEnqueueBatch)); err != nil {
		t.Errorf("WithMaxEnqueueBatch(%d) was rejected: %v", postgres.MaxEnqueueBatch, err)
	}
}
