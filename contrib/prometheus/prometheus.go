// Package prometheus exposes outbox runtime signals as Prometheus metrics.
//
// It is a separate Go module, so the core outbox package has no Prometheus
// dependency: import it only if you use it.
//
//	reg := prometheus.NewRegistry()
//	obs, err := outboxprom.New(reg)
//	if err != nil {
//		return err
//	}
//	worker, err := outbox.NewWorker(store, publisher, outbox.Config{Observer: obs})
//
// # Cardinality
//
// The only label is "type" (the message type) on the publish, retry and
// failure metrics, plus a small closed set of outcome, reason and operation
// labels. Message ids, aggregate ids, payloads and error strings are never
// used as labels. If your message types are not a bounded set — if they
// contain an id, say — wrap the observer with [Options.TypeLabel] to map them
// onto a bounded set or drop them.
package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tahaNGU/gocast"
)

// Options configures the collector.
type Options struct {
	// Namespace and Subsystem prefix every metric name. They default to ""
	// and "outbox", producing names like "outbox_published_total".
	Namespace string
	Subsystem string

	// TypeLabel maps a message type onto the value used as the "type" label.
	// Use it to bound cardinality: return a fixed string to drop the
	// distinction entirely, or map unknown types onto "other". Defaults to the
	// identity function.
	TypeLabel func(string) string

	// DurationBuckets are the histogram buckets for publish duration, in
	// seconds. Defaults to buckets from 1ms to 30s.
	DurationBuckets []float64

	// LagBuckets are the histogram buckets for publication lag — how long a
	// message waited after becoming available — in seconds. Defaults to
	// buckets from 10ms to 30 minutes, because lag is what tells you the
	// outbox is falling behind.
	LagBuckets []float64
}

// Observer implements [outbox.Observer] on top of Prometheus metrics.
type Observer struct {
	outbox.NopObserver // future-proofs against new Observer methods

	typeLabel func(string) string

	claims       *prometheus.CounterVec
	claimed      prometheus.Counter
	expired      prometheus.Counter
	claimSeconds prometheus.Histogram

	published  *prometheus.CounterVec
	retries    *prometheus.CounterVec
	failed     *prometheus.CounterVec
	storeFails *prometheus.CounterVec
	leaseLost  *prometheus.CounterVec

	publishSeconds *prometheus.HistogramVec
	lagSeconds     *prometheus.HistogramVec
	batchSize      prometheus.Histogram
	batchSeconds   prometheus.Histogram

	active  prometheus.Gauge
	pending prometheus.Gauge
	leased  prometheus.Gauge
	failedG prometheus.Gauge
	oldest  prometheus.Gauge
}

// New builds an observer and registers its metrics with reg. Pass
// prometheus.DefaultRegisterer to use the global registry.
func New(reg prometheus.Registerer, opts ...Options) (*Observer, error) {
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Subsystem == "" && o.Namespace == "" {
		o.Subsystem = "outbox"
	}
	if o.TypeLabel == nil {
		o.TypeLabel = func(s string) string { return s }
	}
	if o.DurationBuckets == nil {
		o.DurationBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	}
	if o.LagBuckets == nil {
		o.LagBuckets = []float64{.01, .05, .1, .5, 1, 5, 15, 30, 60, 300, 900, 1800}
	}

	n := func(name string) prometheus.CounterOpts {
		return prometheus.CounterOpts{Namespace: o.Namespace, Subsystem: o.Subsystem, Name: name}
	}
	g := func(name, help string) prometheus.GaugeOpts {
		return prometheus.GaugeOpts{Namespace: o.Namespace, Subsystem: o.Subsystem, Name: name, Help: help}
	}
	h := func(name, help string, buckets []float64) prometheus.HistogramOpts {
		return prometheus.HistogramOpts{
			Namespace: o.Namespace, Subsystem: o.Subsystem, Name: name,
			Help: help, Buckets: buckets,
		}
	}

	obs := &Observer{typeLabel: o.TypeLabel}

	co := n("claims_total")
	co.Help = "Claim queries run, by whether they found work."
	obs.claims = prometheus.NewCounterVec(co, []string{"result"})

	cm := n("claimed_messages_total")
	cm.Help = "Messages leased to this worker."
	obs.claimed = prometheus.NewCounter(cm)

	ex := n("expired_leases_total")
	ex.Help = "Messages reclaimed from a lease that had expired, meaning an earlier attempt did not finish cleanly."
	obs.expired = prometheus.NewCounter(ex)

	obs.claimSeconds = prometheus.NewHistogram(h("claim_duration_seconds",
		"Time spent in the claim query.", o.DurationBuckets))

	pb := n("published_total")
	pb.Help = "Delivery attempts by outcome (success, retryable, permanent, abandoned)."
	obs.published = prometheus.NewCounterVec(pb, []string{"type", "outcome"})

	rt := n("retries_scheduled_total")
	rt.Help = "Retries scheduled after a retryable failure."
	obs.retries = prometheus.NewCounterVec(rt, []string{"type"})

	fl := n("failed_total")
	fl.Help = "Messages that reached the terminal failed state, by reason (permanent, exhausted, undecodable)."
	obs.failed = prometheus.NewCounterVec(fl, []string{"type", "reason"})

	sf := n("store_errors_total")
	sf.Help = "Failed store operations, by operation."
	obs.storeFails = prometheus.NewCounterVec(sf, []string{"operation"})

	ll := n("lease_lost_total")
	ll.Help = "Store updates rejected because the worker's lease had been reclaimed."
	obs.leaseLost = prometheus.NewCounterVec(ll, []string{"operation"})

	obs.publishSeconds = prometheus.NewHistogramVec(h("publish_duration_seconds",
		"Time spent inside Publisher.Publish.", o.DurationBuckets), []string{"type", "outcome"})
	obs.lagSeconds = prometheus.NewHistogramVec(h("publish_lag_seconds",
		"Delay between a message becoming available and its delivery attempt starting.",
		o.LagBuckets), []string{"type"})
	obs.batchSize = prometheus.NewHistogram(h("batch_size",
		"Messages per claimed batch.", []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512}))
	obs.batchSeconds = prometheus.NewHistogram(h("batch_duration_seconds",
		"Time to process a whole claimed batch.", o.DurationBuckets))

	obs.active = prometheus.NewGauge(g("active_publishes", "Delivery attempts currently in flight in this process."))
	obs.pending = prometheus.NewGauge(g("pending_messages", "Messages waiting to be published, as of the last sample."))
	obs.leased = prometheus.NewGauge(g("leased_messages", "Messages currently leased by any worker, as of the last sample."))
	obs.failedG = prometheus.NewGauge(g("failed_messages", "Messages in the terminal failed state, as of the last sample."))
	obs.oldest = prometheus.NewGauge(g("oldest_pending_age_seconds",
		"How long the oldest due message has been waiting, as of the last sample. The primary backlog alert."))

	for _, c := range obs.collectors() {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return obs, nil
}

func (o *Observer) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		o.claims, o.claimed, o.expired, o.claimSeconds,
		o.published, o.retries, o.failed, o.storeFails, o.leaseLost,
		o.publishSeconds, o.lagSeconds, o.batchSize, o.batchSeconds,
		o.active, o.pending, o.leased, o.failedG, o.oldest,
	}
}

// Describe implements [prometheus.Collector], so the observer can also be
// registered as a single collector.
func (o *Observer) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range o.collectors() {
		c.Describe(ch)
	}
}

// Collect implements [prometheus.Collector].
func (o *Observer) Collect(ch chan<- prometheus.Metric) {
	for _, c := range o.collectors() {
		c.Collect(ch)
	}
}

// ClaimCompleted implements [outbox.Observer].
func (o *Observer) ClaimCompleted(_ context.Context, claimed, expiredLeases int, d time.Duration) {
	result := "empty"
	if claimed > 0 {
		result = "found"
	}
	o.claims.WithLabelValues(result).Inc()
	o.claimSeconds.Observe(d.Seconds())
	if claimed > 0 {
		o.claimed.Add(float64(claimed))
	}
	if expiredLeases > 0 {
		o.expired.Add(float64(expiredLeases))
	}
}

// PublishCompleted implements [outbox.Observer].
func (o *Observer) PublishCompleted(_ context.Context, e outbox.PublishEvent) {
	typ := o.typeLabel(e.Type)
	outcome := string(e.Outcome)
	o.published.WithLabelValues(typ, outcome).Inc()
	o.publishSeconds.WithLabelValues(typ, outcome).Observe(e.Duration.Seconds())
	o.lagSeconds.WithLabelValues(typ).Observe(e.Lag.Seconds())
}

// RetryScheduled implements [outbox.Observer].
func (o *Observer) RetryScheduled(_ context.Context, msgType string, _ int, _ time.Duration) {
	o.retries.WithLabelValues(o.typeLabel(msgType)).Inc()
}

// MessageFailed implements [outbox.Observer].
func (o *Observer) MessageFailed(_ context.Context, msgType string, reason outbox.FailureReason, _ int) {
	o.failed.WithLabelValues(o.typeLabel(msgType), string(reason)).Inc()
}

// BatchCompleted implements [outbox.Observer].
func (o *Observer) BatchCompleted(_ context.Context, size int, d time.Duration) {
	o.batchSize.Observe(float64(size))
	o.batchSeconds.Observe(d.Seconds())
}

// StoreFailed implements [outbox.Observer].
func (o *Observer) StoreFailed(_ context.Context, op string) {
	o.storeFails.WithLabelValues(op).Inc()
}

// LeaseLost implements [outbox.Observer].
func (o *Observer) LeaseLost(_ context.Context, op string) {
	o.leaseLost.WithLabelValues(op).Inc()
}

// WorkersActive implements [outbox.Observer].
func (o *Observer) WorkersActive(_ context.Context, n int) {
	o.active.Set(float64(n))
}

// StatsSampled implements [outbox.Observer].
func (o *Observer) StatsSampled(_ context.Context, s outbox.Stats) {
	o.pending.Set(float64(s.Pending))
	o.leased.Set(float64(s.Leased))
	o.failedG.Set(float64(s.Failed))
	o.oldest.Set(s.OldestPendingAge.Seconds())
}

var (
	_ outbox.Observer      = (*Observer)(nil)
	_ prometheus.Collector = (*Observer)(nil)
)
