package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/tahaNGU/gocast"
	"github.com/tahaNGU/gocast/memstore"
)

// The outbox invariant: a business change and its event commit together, or
// neither does. This example uses the in-memory store so that it runs; a real
// application passes its own *sql.Tx to the Postgres store's Enqueue.
func Example() {
	store := memstore.New()
	ctx := context.Background()

	// A transaction that succeeds: the event is stored with the change.
	tx := store.Begin()
	if err := store.Enqueue(ctx, tx, outbox.Message{
		Type:        "orders.created.v1",
		Payload:     []byte(`{"order_id":"o-1"}`),
		ContentType: "application/json",
	}); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}

	// A transaction that fails: the event disappears with it.
	tx = store.Begin()
	if err := store.Enqueue(ctx, tx, outbox.Message{Type: "orders.created.v1"}); err != nil {
		panic(err)
	}
	if err := tx.Rollback(); err != nil {
		panic(err)
	}

	fmt.Println("stored messages:", store.Len())
	// Output: stored messages: 1
}

// A worker publishes stored messages in the background, and is stopped with a
// bounded deadline.
func ExampleWorker() {
	store := memstore.New()
	ctx := context.Background()
	if err := store.Enqueue(ctx, nil, outbox.Message{Type: "orders.created.v1"}); err != nil {
		panic(err)
	}

	delivered := make(chan string, 1)
	publisher := outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		// Hand m to a broker here, using m.ID as the deduplication key.
		delivered <- m.Type
		return nil
	})

	worker, err := outbox.NewWorker(store, publisher, outbox.Config{
		Concurrency:  1,
		PollInterval: time.Millisecond,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		panic(err)
	}
	if err := worker.Start(ctx); err != nil {
		panic(err)
	}
	fmt.Println("published:", <-delivered)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := worker.Shutdown(shutdownCtx); err != nil {
		panic(err)
	}
	fmt.Println("stopped cleanly")
	// Output:
	// published: orders.created.v1
	// stopped cleanly
}

// Wrap an error with Permanent when retrying cannot help: the message is moved
// to the failed state instead of consuming its retry budget.
func ExamplePermanent() {
	publish := func(topic string) error {
		if topic == "" {
			return outbox.Permanent(errors.New("no topic configured"))
		}
		return errors.New("connection refused")
	}

	permanent := publish("")
	retryable := publish("orders")

	fmt.Println(outbox.Classify(permanent))
	fmt.Println(outbox.Classify(retryable))
	fmt.Println(errors.Is(permanent, errors.Unwrap(permanent))) // the cause is preserved
	// Output:
	// permanent
	// retryable
	// true
}

// Jitter is disabled here so the schedule is exact; the default adds +/-20%.
func ExampleBackoff_Delay() {
	b := outbox.Backoff{
		Initial:    time.Second,
		Max:        10 * time.Second,
		Multiplier: 2,
	}
	for attempt := 1; attempt <= 5; attempt++ {
		fmt.Printf("after attempt %d: wait %s\n", attempt, b.Delay(attempt))
	}
	// Output:
	// after attempt 1: wait 1s
	// after attempt 2: wait 2s
	// after attempt 3: wait 4s
	// after attempt 4: wait 8s
	// after attempt 5: wait 10s
}

// Message ids are UUIDv7: unique, time-ordered, and stable across retries, so
// consumers can use them to deduplicate.
func ExampleNewID() {
	id := outbox.NewID()
	parsed, err := outbox.ParseID(id.String())
	if err != nil {
		panic(err)
	}
	created, ok := parsed.Time()

	fmt.Println("round trips:", parsed == id)
	fmt.Println("carries a timestamp:", ok && time.Since(created) < time.Minute)
	fmt.Println("length:", len(id.String()))
	// Output:
	// round trips: true
	// carries a timestamp: true
	// length: 36
}
