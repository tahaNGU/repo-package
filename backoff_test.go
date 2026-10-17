package outbox

import (
	"errors"
	"testing"
	"time"
)

func TestBackoffDelayIsExponentialAndCapped(t *testing.T) {
	b := Backoff{Initial: time.Second, Max: 10 * time.Second, Multiplier: 2} // no jitter
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		10 * time.Second, 10 * time.Second,
	}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w {
			t.Errorf("Delay(%d) = %s, want %s", i+1, got, w)
		}
	}
	if got := b.Delay(0); got != time.Second {
		t.Errorf("Delay(0) = %s, want the first delay", got)
	}
	if got := b.Delay(-5); got != time.Second {
		t.Errorf("Delay(-5) = %s, want the first delay", got)
	}
}

func TestBackoffDelayDoesNotOverflow(t *testing.T) {
	b := Backoff{Initial: time.Second, Max: time.Minute, Multiplier: 10}
	for _, attempt := range []int{100, 1000, 1 << 20} {
		got := b.Delay(attempt)
		if got != time.Minute {
			t.Fatalf("Delay(%d) = %s, want the cap %s", attempt, got, time.Minute)
		}
	}
}

func TestBackoffJitterStaysInBounds(t *testing.T) {
	b := Backoff{Initial: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 0.5}
	var sawBelow, sawAbove bool
	for range 500 {
		got := b.Delay(3) // base 4s, so [2s, 6s]
		if got < 2*time.Second || got > 6*time.Second {
			t.Fatalf("Delay with jitter = %s, outside [2s, 6s]", got)
		}
		if got < 4*time.Second {
			sawBelow = true
		}
		if got > 4*time.Second {
			sawAbove = true
		}
	}
	if !sawBelow || !sawAbove {
		t.Error("jitter did not randomise in both directions")
	}
}

func TestBackoffDefaults(t *testing.T) {
	got := Backoff{}.withDefaults()
	if got != DefaultBackoff() {
		t.Fatalf("zero backoff defaults to %+v, want %+v", got, DefaultBackoff())
	}
	custom := Backoff{Initial: 5 * time.Second}.withDefaults()
	if custom.Initial != 5*time.Second || custom.Max != DefaultBackoff().Max {
		t.Fatalf("partial backoff = %+v, want only Initial overridden", custom)
	}
}

func TestBackoffValidate(t *testing.T) {
	for name, b := range map[string]Backoff{
		"negative initial":   {Initial: -time.Second},
		"negative max":       {Max: -time.Second},
		"initial beyond max": {Initial: time.Minute, Max: time.Second},
		"multiplier below 1": {Multiplier: 0.5},
		"jitter above 1":     {Jitter: 1.5},
		"negative jitter":    {Jitter: -0.1},
	} {
		if err := b.validate(); err == nil {
			t.Errorf("%s: validate accepted %+v", name, b)
		} else if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: error %v does not wrap ErrInvalidConfig", name, err)
		}
	}
	if err := (Backoff{}).validate(); err != nil {
		t.Errorf("zero backoff rejected: %v", err)
	}
}
