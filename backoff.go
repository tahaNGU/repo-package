package outbox

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Backoff computes the delay before the next delivery attempt. The schedule is
// exponential and capped:
//
//	delay(attempt) = min(Initial * Multiplier^(attempt-1), Max)
//
// and is then randomised by up to Jitter in both directions, so that many
// workers retrying the same backlog do not synchronise.
//
// The delay is persisted as the message's availability time, so the schedule
// survives process restarts: a message that was waiting five minutes before a
// deployment is still waiting five minutes after it.
type Backoff struct {
	// Initial is the delay after the first failed attempt. Defaults to 1s.
	Initial time.Duration
	// Max caps the delay. Defaults to 5m.
	Max time.Duration
	// Multiplier grows the delay; it must be >= 1. Defaults to 2.
	Multiplier float64
	// Jitter is the fraction of the delay to randomise, in [0, 1]. 0 disables
	// randomisation and makes the schedule deterministic. Defaults to 0.2.
	Jitter float64
}

// DefaultBackoff is the schedule used when none is configured: 1s, 2s, 4s, …
// capped at 5 minutes, with 20% jitter.
func DefaultBackoff() Backoff {
	return Backoff{Initial: time.Second, Max: 5 * time.Minute, Multiplier: 2, Jitter: 0.2}
}

func (b Backoff) withDefaults() Backoff {
	d := DefaultBackoff()
	if b.Initial > 0 {
		d.Initial = b.Initial
	}
	if b.Max > 0 {
		d.Max = b.Max
	}
	if b.Multiplier > 0 {
		d.Multiplier = b.Multiplier
	}
	if b.Jitter != 0 {
		d.Jitter = b.Jitter
	}
	return d
}

func (b Backoff) validate() error {
	switch {
	case b.Initial < 0:
		return fmt.Errorf("%w: backoff initial must not be negative", ErrInvalidConfig)
	case b.Max < 0:
		return fmt.Errorf("%w: backoff max must not be negative", ErrInvalidConfig)
	case b.Initial > 0 && b.Max > 0 && b.Initial > b.Max:
		return fmt.Errorf("%w: backoff initial %s exceeds max %s", ErrInvalidConfig, b.Initial, b.Max)
	case b.Multiplier != 0 && b.Multiplier < 1:
		return fmt.Errorf("%w: backoff multiplier must be >= 1", ErrInvalidConfig)
	case b.Jitter < 0 || b.Jitter > 1:
		return fmt.Errorf("%w: backoff jitter must be in [0,1]", ErrInvalidConfig)
	}
	return nil
}

// Delay returns the wait before attempt+1 given that `attempt` attempts have
// already failed. Attempts below 1 are treated as 1. The result is never
// negative.
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := float64(b.Initial) * math.Pow(b.Multiplier, float64(attempt-1))
	if cap := float64(b.Max); base > cap || math.IsInf(base, 0) {
		base = cap
	}
	if b.Jitter > 0 {
		base *= 1 + b.Jitter*(2*rand.Float64()-1)
	}
	if base < 0 {
		return 0
	}
	return time.Duration(base)
}
