package prometheus_test

import (
	"context"
	"strings"
	"testing"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"

	"github.com/tahaNGU/gocast"
	outboxprom "github.com/tahaNGU/gocast/contrib/prometheus"
)

// metricValue sums the samples of a counter or gauge family, and seriesCount
// reports how many label combinations it has. Reading the registry directly
// keeps this module's test dependencies to the client library itself.
func metricValue(t *testing.T, reg *promclient.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range f.GetMetric() {
			switch {
			case m.GetCounter() != nil:
				sum += m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				sum += m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				sum += float64(m.GetHistogram().GetSampleCount())
			}
		}
		return sum
	}
	t.Fatalf("metric %s was not registered", name)
	return 0
}

func seriesCount(t *testing.T, reg *promclient.Registry, name string) int {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return len(f.GetMetric())
		}
	}
	t.Fatalf("metric %s was not registered", name)
	return 0
}

func TestObserverRecordsEverySignal(t *testing.T) {
	reg := promclient.NewRegistry()
	obs, err := outboxprom.New(reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	obs.ClaimCompleted(ctx, 5, 2, 3*time.Millisecond)
	obs.ClaimCompleted(ctx, 0, 0, time.Millisecond)
	obs.PublishCompleted(ctx, outbox.PublishEvent{
		Type: "orders.created.v1", Outcome: outbox.OutcomeSuccess,
		Attempt: 1, Duration: 10 * time.Millisecond, Lag: 2 * time.Second,
	})
	obs.PublishCompleted(ctx, outbox.PublishEvent{
		Type: "orders.created.v1", Outcome: outbox.OutcomeRetryable, Attempt: 2,
	})
	obs.RetryScheduled(ctx, "orders.created.v1", 2, time.Second)
	obs.MessageFailed(ctx, "orders.created.v1", outbox.ReasonExhausted, 10)
	obs.BatchCompleted(ctx, 5, 50*time.Millisecond)
	obs.StoreFailed(ctx, "claim")
	obs.LeaseLost(ctx, "publish_ack")
	obs.WorkersActive(ctx, 3)
	obs.StatsSampled(ctx, outbox.Stats{Pending: 7, Leased: 3, Failed: 1, OldestPendingAge: 90 * time.Second})

	for name, want := range map[string]float64{
		"outbox_claimed_messages_total":     5,
		"outbox_expired_leases_total":       2,
		"outbox_retries_scheduled_total":    1,
		"outbox_failed_total":               1,
		"outbox_store_errors_total":         1,
		"outbox_lease_lost_total":           1,
		"outbox_active_publishes":           3,
		"outbox_pending_messages":           7,
		"outbox_leased_messages":            3,
		"outbox_failed_messages":            1,
		"outbox_oldest_pending_age_seconds": 90,
		"outbox_published_total":            2,
		"outbox_claims_total":               2,
		"outbox_claim_duration_seconds":     2, // sample count
		"outbox_publish_duration_seconds":   2,
		"outbox_publish_lag_seconds":        2,
		"outbox_batch_size":                 1,
		"outbox_batch_duration_seconds":     1,
	} {
		if got := metricValue(t, reg, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if n := seriesCount(t, reg, "outbox_published_total"); n != 2 {
		t.Errorf("published_total has %d series, want one per outcome", n)
	}
	if n := seriesCount(t, reg, "outbox_claims_total"); n != 2 {
		t.Errorf("claims_total has %d series, want found and empty", n)
	}
}

func TestLabelsStayBounded(t *testing.T) {
	reg := promclient.NewRegistry()
	obs, err := outboxprom.New(reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	for i := range 50 {
		obs.PublishCompleted(ctx, outbox.PublishEvent{
			Type:    "type-" + string(rune('a'+i%5)),
			Outcome: outbox.OutcomeSuccess,
		})
		obs.StoreFailed(ctx, "claim")
	}
	if n := seriesCount(t, reg, "outbox_published_total"); n != 5 {
		t.Errorf("published_total has %d series, want 5 (one per type)", n)
	}
	if n := seriesCount(t, reg, "outbox_store_errors_total"); n != 1 {
		t.Errorf("store_errors_total has %d series, want 1", n)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	allowed := map[string]bool{"type": true, "outcome": true, "reason": true, "operation": true, "result": true}
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "outbox_") {
			t.Errorf("metric %q is not namespaced", f.GetName())
		}
		if f.GetHelp() == "" {
			t.Errorf("metric %q has no help text", f.GetName())
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if !allowed[l.GetName()] {
					t.Errorf("%s has an unexpected label %q", f.GetName(), l.GetName())
				}
				if len(l.GetValue()) > 64 {
					t.Errorf("%s label %s has a suspiciously long value %q", f.GetName(), l.GetName(), l.GetValue())
				}
			}
		}
	}
}

func TestTypeLabelCanCollapseCardinality(t *testing.T) {
	reg := promclient.NewRegistry()
	obs, err := outboxprom.New(reg, outboxprom.Options{
		TypeLabel: func(string) string { return "all" },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	for i := range 20 {
		obs.PublishCompleted(ctx, outbox.PublishEvent{
			Type:    "unbounded-" + string(rune('a'+i)),
			Outcome: outbox.OutcomeSuccess,
		})
	}
	if n := seriesCount(t, reg, "outbox_published_total"); n != 1 {
		t.Fatalf("published_total has %d series, want 1 after collapsing the type label", n)
	}
}

func TestNamespaceOption(t *testing.T) {
	reg := promclient.NewRegistry()
	obs, err := outboxprom.New(reg, outboxprom.Options{Namespace: "myapp", Subsystem: "events"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	obs.WorkersActive(context.Background(), 1)
	if got := metricValue(t, reg, "myapp_events_active_publishes"); got != 1 {
		t.Fatalf("namespaced metric = %v, want 1", got)
	}
}

func TestDoubleRegistrationIsReported(t *testing.T) {
	reg := promclient.NewRegistry()
	if _, err := outboxprom.New(reg); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if _, err := outboxprom.New(reg); err == nil {
		t.Fatal("registering twice on one registry must fail")
	}
}

func TestObserverIsUsableAsBothInterfaces(t *testing.T) {
	obs, err := outboxprom.New(promclient.NewRegistry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var _ outbox.Observer = obs
	var _ promclient.Collector = obs
	// The worker must accept it as its Observer.
	if _, err := outbox.NewWorker(nil, nil, outbox.Config{Observer: obs}); err == nil {
		t.Fatal("expected the nil store to be rejected")
	}
}
