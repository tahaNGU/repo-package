// Command basic is a complete, runnable example of the transactional outbox:
// it creates a business table and an outbox table, writes to both in one
// transaction, and runs a worker that publishes the stored messages.
//
// Run it against any PostgreSQL instance:
//
//	go run ./examples/basic -dsn 'postgres://user:pass@localhost:5432/db?sslmode=disable'
//
// It is deliberately a single file: everything an application has to do is
// visible in one place.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/tahaNGU/gocast"
	"github.com/tahaNGU/gocast/postgres"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "PostgreSQL connection string")
	orders := flag.Int("orders", 5, "how many orders to place")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "a -dsn (or DATABASE_URL) is required")
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	if err := run(*dsn, *orders, logger); err != nil {
		logger.Error("example failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(dsn string, orders int, logger *slog.Logger) error {
	// The application context: cancelled on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()
	// Leave room for the worker's own queries: it needs up to Concurrency
	// connections for acknowledgements plus one for polling.
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	store, err := postgres.New(db)
	if err != nil {
		return fmt.Errorf("build store: %w", err)
	}
	// In a real project, apply the migration with your own tool instead; see
	// postgres.Migrations.
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate outbox: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS example_orders (
		id text PRIMARY KEY,
		total_cents bigint NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now(),
		CONSTRAINT example_orders_total_check CHECK (total_cents > 0))`); err != nil {
		return fmt.Errorf("migrate business table: %w", err)
	}

	// The publisher is the only piece an application has to write. A real one
	// would hand the message to Kafka, NATS, RabbitMQ or an HTTP endpoint.
	published := make(chan outbox.ID, orders)
	publisher := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		// Honour ctx: the worker uses it to bound the attempt and to stop
		// cleanly on shutdown.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond): // pretend to be a broker
		}
		logger.Info("published",
			slog.String("id", m.ID.String()),
			slog.String("type", m.Type),
			slog.String("aggregate", m.AggregateID),
			slog.Int("bytes", len(m.Payload)))
		select {
		case published <- m.ID:
		default:
		}
		return nil
	})

	worker, err := outbox.NewWorker(store, publisher, outbox.Config{
		Concurrency:    4,
		BatchSize:      32,
		PollInterval:   250 * time.Millisecond,
		LeaseDuration:  30 * time.Second,
		PublishTimeout: 5 * time.Second,
		MaxAttempts:    8,
		Retention:      24 * time.Hour, // delete published rows after a day
		Logger:         logger,
		// Observer: an outbox.Observer, e.g. contrib/prometheus
		// Tracer:   an outbox.Tracer, e.g. contrib/otel
	})
	if err != nil {
		return fmt.Errorf("build worker: %w", err)
	}
	if err := worker.Start(ctx); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}
	// Always stop the worker with a bounded deadline.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := worker.Shutdown(shutdownCtx); err != nil {
			logger.Error("outbox shutdown was not clean", slog.Any("error", err))
		}
	}()

	for i := range orders {
		id := fmt.Sprintf("order-%d-%d", time.Now().UnixNano(), i)
		if err := placeOrder(ctx, db, store, id, int64(1999+i*500)); err != nil {
			return fmt.Errorf("place order %s: %w", id, err)
		}
		// Optional: wake the poller instead of waiting out the poll interval.
		worker.Notify()
	}

	// Show the rollback guarantee. This order violates a CHECK constraint, so
	// the transaction fails after Enqueue has already inserted its outbox row
	// -- and the event disappears with the order.
	before, err := store.Stats(ctx)
	if err != nil {
		return fmt.Errorf("stats: %w", err)
	}
	if err := placeOrder(ctx, db, store, "order-free-lunch", 0); err == nil {
		return errors.New("expected the invalid order to be rejected")
	} else {
		logger.Info("the rolled-back order produced no outbox row", slog.Any("error", err))
	}
	after, err := store.Stats(ctx)
	if err != nil {
		return fmt.Errorf("stats: %w", err)
	}
	if total := after.Pending + after.Leased + after.Failed; total > before.Pending+before.Leased+before.Failed {
		return fmt.Errorf("a rolled-back transaction left %d unpublished messages behind", total)
	}

	// Wait for the worker to drain what we enqueued.
	deadline := time.After(30 * time.Second)
	for range orders {
		select {
		case <-published:
		case <-ctx.Done():
			return nil
		case <-deadline:
			return errors.New("timed out waiting for publication")
		}
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		return fmt.Errorf("stats: %w", err)
	}
	logger.Info("outbox drained",
		slog.Int64("pending", stats.Pending),
		slog.Int64("leased", stats.Leased),
		slog.Int64("failed", stats.Failed),
		slog.Duration("oldest_pending_age", stats.OldestPendingAge))
	return nil
}

// placeOrder is the whole point of the package: a business write and its event
// in one transaction.
func placeOrder(ctx context.Context, db *sql.DB, store *postgres.Store, id string, totalCents int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Safe to call after a successful Commit: it is then a no-op.
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO example_orders (id, total_cents) VALUES ($1, $2)`, id, totalCents); err != nil {
		return err
	}

	payload, err := json.Marshal(map[string]any{
		"order_id":    id,
		"total_cents": totalCents,
		"placed_at":   time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	// Setting the id explicitly means we know it before the commit, which is
	// useful for logs and as a broker deduplication key.
	msg := outbox.Message{
		ID:            outbox.NewID(),
		Type:          "orders.created.v1",
		Payload:       payload,
		ContentType:   "application/json",
		AggregateType: "order",
		AggregateID:   id,
		Headers:       map[string]string{"source": "examples/basic"},
	}
	if err := store.Enqueue(ctx, tx, msg); err != nil {
		return err
	}
	return tx.Commit()
}
