package outbox

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConfigZeroValueIsValid(t *testing.T) {
	if err := (Config{}).Validate(); err != nil {
		t.Fatalf("zero config rejected: %v", err)
	}
	c := Config{}.withDefaults()
	if c.Concurrency != DefaultConcurrency || c.BatchSize != DefaultBatchSize ||
		c.PollInterval != DefaultPollInterval || c.LeaseDuration != DefaultLeaseDuration ||
		c.PublishTimeout != DefaultPublishTimeout || c.StoreTimeout != DefaultStoreTimeout ||
		c.MaxAttempts != DefaultMaxAttempts {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if c.Name == "" || !strings.Contains(c.Name, "/") {
		t.Errorf("default name = %q, want host/pid", c.Name)
	}
	if c.Logger == nil || c.Observer == nil || c.Classify == nil || c.ErrorSummary == nil {
		t.Error("defaults left a required hook nil")
	}
	if c.Tracer != nil {
		t.Error("tracing must stay opt-in")
	}
}

func TestConfigRejectsLeaseShorterThanPublishWindow(t *testing.T) {
	err := Config{
		LeaseDuration:  2 * time.Second,
		PublishTimeout: 5 * time.Second,
		StoreTimeout:   time.Second,
	}.Validate()
	if err == nil {
		t.Fatal("accepted a lease shorter than publish timeout + store timeout")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error %v does not wrap ErrInvalidConfig", err)
	}
	if !strings.Contains(err.Error(), "lease") {
		t.Fatalf("error %v should explain the lease relationship", err)
	}
}

func TestConfigRejectsNegativeValues(t *testing.T) {
	cases := map[string]Config{
		"concurrency":     {Concurrency: -1},
		"batch size":      {BatchSize: -1},
		"poll interval":   {PollInterval: -time.Second},
		"lease":           {LeaseDuration: -time.Second},
		"publish timeout": {PublishTimeout: -time.Second},
		"store timeout":   {StoreTimeout: -time.Second},
		"max attempts":    {MaxAttempts: -1},
		"retention":       {Retention: -time.Hour},
		"prune batch":     {PruneBatch: -1},
		"prune interval":  {PruneInterval: -time.Minute},
		"backoff":         {Backoff: Backoff{Jitter: 2}},
	}
	for name, c := range cases {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted an invalid value", name)
		} else if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: error %v does not wrap ErrInvalidConfig", name, err)
		}
	}
}

func TestConfigReportsEveryProblemAtOnce(t *testing.T) {
	err := Config{Concurrency: -1, BatchSize: -2, MaxAttempts: -3}.Validate()
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"concurrency", "batch size", "max attempts"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error %q is missing %q", err, want)
		}
	}
}

func TestConfigNegativeStatsIntervalDisablesSampling(t *testing.T) {
	c := Config{StatsInterval: -1}
	if err := c.Validate(); err != nil {
		t.Fatalf("a negative stats interval should disable sampling, not fail: %v", err)
	}
	if got := c.withDefaults().StatsInterval; got != -1 {
		t.Fatalf("StatsInterval = %s, want the explicit -1 preserved", got)
	}
}
