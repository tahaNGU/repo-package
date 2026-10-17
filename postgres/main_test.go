package postgres_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/tahaNGU/gocast"
	"github.com/tahaNGU/gocast/postgres"
)

// DSNEnv names the environment variable that points the integration tests at a
// PostgreSQL instance. Without it they skip, so `go test ./...` still works on
// a machine with no database.
const DSNEnv = "OUTBOX_TEST_POSTGRES_DSN"

var sharedDB *sql.DB

func TestMain(m *testing.M) {
	flag.Parse()
	dsn := os.Getenv(DSNEnv)
	if dsn == "" {
		fmt.Fprintf(os.Stderr, "%s is not set: skipping PostgreSQL integration tests\n", DSNEnv)
		os.Exit(m.Run())
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot open %s: %v\n", DSNEnv, err)
		os.Exit(1)
	}
	// Integration tests run many workers at once; give them room.
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "cannot reach the test database: %v\n", err)
		os.Exit(1)
	}
	sharedDB = db
	code := m.Run()
	_ = db.Close()
	os.Exit(code)
}

var tableSeq atomic.Int64

// newStore creates an isolated outbox table for one test and returns a store
// for it. The table is dropped when the test ends.
func newStore(t *testing.T, opts ...postgres.Option) (*postgres.Store, *sql.DB) {
	t.Helper()
	if sharedDB == nil {
		t.Skipf("%s is not set", DSNEnv)
	}
	table := fmt.Sprintf("outbox_test_%d_%d", time.Now().UnixNano()%1_000_000, tableSeq.Add(1))
	opts = append([]postgres.Option{postgres.WithTable(table)}, opts...)
	store, err := postgres.New(sharedDB, opts...)
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := sharedDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Logf("dropping %s: %v", table, err)
		}
	})
	return store, sharedDB
}

func requireNoError(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// row reads the raw columns of one message, so tests can assert on what is
// actually stored rather than on what the store reports.
type row struct {
	State       string
	Attempts    int
	LeaseEpoch  int64
	LeaseOwner  sql.NullString
	AvailableAt time.Time
	PublishedAt sql.NullTime
	CreatedAt   time.Time
	LastError   sql.NullString
	ErrorKind   sql.NullString
	Headers     string
	Payload     []byte
	OrderingKey sql.NullString
	Seq         int64
}

func readRow(t *testing.T, store *postgres.Store, id outbox.ID) row {
	t.Helper()
	var r row
	err := sharedDB.QueryRowContext(context.Background(), `
		SELECT state, attempts, lease_epoch, lease_owner,
		       available_at, published_at, created_at, last_error, last_error_kind,
		       headers::text, payload, ordering_key, seq
		  FROM `+store.Table()+` WHERE id = $1`, id).
		Scan(&r.State, &r.Attempts, &r.LeaseEpoch, &r.LeaseOwner,
			&r.AvailableAt, &r.PublishedAt, &r.CreatedAt, &r.LastError, &r.ErrorKind,
			&r.Headers, &r.Payload, &r.OrderingKey, &r.Seq)
	requireNoError(t, err, "read row "+id.String())
	return r
}

func countState(t *testing.T, store *postgres.Store, state outbox.State) int {
	t.Helper()
	var n int
	err := sharedDB.QueryRowContext(context.Background(),
		"SELECT count(*) FROM "+store.Table()+" WHERE state = $1", string(state)).Scan(&n)
	requireNoError(t, err, "count "+string(state))
	return n
}

func enqueueCommitted(t *testing.T, store *postgres.Store, msgs ...outbox.Message) []outbox.ID {
	t.Helper()
	ctx := context.Background()
	for i := range msgs {
		if msgs[i].ID.IsZero() {
			msgs[i].ID = outbox.NewID()
		}
		if msgs[i].Type == "" {
			msgs[i].Type = "test.event.v1"
		}
	}
	tx, err := sharedDB.BeginTx(ctx, nil)
	requireNoError(t, err, "begin")
	defer tx.Rollback()
	requireNoError(t, store.Enqueue(ctx, tx, msgs...), "enqueue")
	requireNoError(t, tx.Commit(), "commit")

	ids := make([]outbox.ID, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
