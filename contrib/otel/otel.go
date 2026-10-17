// Package otel traces outbox publication with OpenTelemetry.
//
// It is a separate Go module, so the core outbox package has no OpenTelemetry
// dependency: import it only if you use it.
//
//	tracer, err := outboxotel.New(otel.GetTracerProvider())
//	worker, err := outbox.NewWorker(store, publisher, outbox.Config{Tracer: tracer})
//
// # Linking the enqueue to the publish
//
// A message is published asynchronously, often seconds or minutes after the
// transaction that produced it and usually in another goroutine, so there is
// no live parent span to attach to. The link travels with the message
// instead: write the current trace context into the message headers at enqueue
// time with [InjectHeaders], and the publish span is created as a child of
// that remote context.
//
//	msg := outbox.Message{Type: "orders.created.v1", Payload: body}
//	msg.Headers = outboxotel.InjectHeaders(ctx, msg.Headers)
//	err := store.Enqueue(ctx, tx, msg)
//
// Without that, publish spans are still produced, just as roots.
//
// # Attributes
//
// Spans carry the message type, attempt number, batch-independent counters and
// the lease epoch: bounded values. The message id is recorded as
// "messaging.message.id" because it is the one high-cardinality value that is
// genuinely needed to follow a single message through a broker; it is a span
// attribute, never a metric label.
package otel

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/tahaNGU/gocast"
)

// ScopeName is the instrumentation scope reported for spans from this package.
const ScopeName = "github.com/tahaNGU/gocast/contrib/otel"

// Tracer implements [outbox.Tracer].
type Tracer struct {
	tracer trace.Tracer
}

// New returns a Tracer that creates spans with tp. Passing a nil provider
// returns an error rather than silently producing no spans.
func New(tp trace.TracerProvider) (*Tracer, error) {
	if tp == nil {
		return nil, errors.New("outbox/contrib/otel: a TracerProvider is required")
	}
	return &Tracer{tracer: tp.Tracer(ScopeName)}, nil
}

// StartPublish implements [outbox.Tracer]. It starts a producer span, using
// the trace context stored in the message headers as the remote parent when
// one is present.
func (t *Tracer) StartPublish(ctx context.Context, m *outbox.Claimed) (context.Context, outbox.EndSpan) {
	if sc, ok := spanContextFromHeaders(m.Headers); ok && !trace.SpanContextFromContext(ctx).IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
	}
	attrs := []attribute.KeyValue{
		attribute.String("messaging.operation.name", "publish"),
		attribute.String("messaging.system", "outbox"),
		attribute.String("messaging.message.id", m.ID.String()),
		attribute.String("outbox.message.type", m.Type),
		attribute.Int("outbox.attempt", m.Attempt),
		attribute.Int64("outbox.lease_epoch", m.LeaseEpoch),
	}
	if m.AggregateType != "" {
		attrs = append(attrs, attribute.String("outbox.aggregate.type", m.AggregateType))
	}
	if m.ReclaimedLease {
		attrs = append(attrs, attribute.Bool("outbox.reclaimed_lease", true))
	}
	ctx, span := t.tracer.Start(ctx, "outbox publish "+m.Type,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...))

	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.Bool("outbox.permanent", outbox.IsPermanent(err)))
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
	}
}

// InjectHeaders writes the trace context of ctx into headers so that the
// eventual publish span can be linked to the transaction that enqueued the
// message. It allocates the map if needed and returns it unchanged when ctx
// carries no valid span.
func InjectHeaders(ctx context.Context, headers map[string]string) map[string]string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return headers
	}
	out, err := outbox.InjectTraceParent(headers, sc.TraceID().String(), sc.SpanID().String(), sc.IsSampled())
	if err != nil {
		return headers
	}
	return out
}

func spanContextFromHeaders(headers map[string]string) (trace.SpanContext, bool) {
	traceID, spanID, sampled, ok := outbox.TraceParent(headers)
	if !ok {
		return trace.SpanContext{}, false
	}
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return trace.SpanContext{}, false
	}
	sid, err := trace.SpanIDFromHex(spanID)
	if err != nil {
		return trace.SpanContext{}, false
	}
	var flags trace.TraceFlags
	if sampled {
		flags = trace.FlagsSampled
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: flags,
		Remote:     true,
	}), true
}

var _ outbox.Tracer = (*Tracer)(nil)
