package otel_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/tahaNGU/gocast"
	outboxotel "github.com/tahaNGU/gocast/contrib/otel"
)

func newTracer(t *testing.T) (*outboxotel.Tracer, *tracetest.SpanRecorder, trace.TracerProvider) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	tr, err := outboxotel.New(tp)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tr, rec, tp
}

func TestNewRejectsNilProvider(t *testing.T) {
	if _, err := outboxotel.New(nil); err == nil {
		t.Fatal("a nil TracerProvider was accepted")
	}
}

func TestStartPublishRecordsASpan(t *testing.T) {
	tr, rec, _ := newTracer(t)
	m := outbox.Claimed{
		Message: outbox.Message{
			ID:            outbox.NewID(),
			Type:          "orders.created.v1",
			AggregateType: "order",
		},
		Attempt:    2,
		LeaseEpoch: 7,
	}
	ctx, end := tr.StartPublish(context.Background(), &m)
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("the returned context carries no span, so a publisher cannot nest under it")
	}
	end(nil)

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	span := spans[0]
	if span.Name() != "outbox publish orders.created.v1" {
		t.Errorf("span name = %q", span.Name())
	}
	if span.SpanKind() != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want producer", span.SpanKind())
	}
	if span.Status().Code != codes.Ok {
		t.Errorf("status = %v, want Ok", span.Status().Code)
	}
	attrs := map[attribute.Key]attribute.Value{}
	for _, kv := range span.Attributes() {
		attrs[kv.Key] = kv.Value
	}
	if got := attrs["messaging.message.id"].AsString(); got != m.ID.String() {
		t.Errorf("messaging.message.id = %q, want %q", got, m.ID)
	}
	if got := attrs["outbox.attempt"].AsInt64(); got != 2 {
		t.Errorf("outbox.attempt = %d, want 2", got)
	}
	if got := attrs["outbox.lease_epoch"].AsInt64(); got != 7 {
		t.Errorf("outbox.lease_epoch = %d, want 7", got)
	}
	if got := attrs["outbox.message.type"].AsString(); got != "orders.created.v1" {
		t.Errorf("outbox.message.type = %q", got)
	}
	if _, ok := attrs["outbox.reclaimed_lease"]; ok {
		t.Error("a fresh claim must not be flagged as reclaimed")
	}
}

func TestStartPublishRecordsFailures(t *testing.T) {
	tr, rec, _ := newTracer(t)
	cause := errors.New("broker refused")
	m := outbox.Claimed{Message: outbox.Message{ID: outbox.NewID(), Type: "t"}, Attempt: 1}

	_, end := tr.StartPublish(context.Background(), &m)
	end(outbox.Permanent(cause))

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	span := spans[0]
	if span.Status().Code != codes.Error {
		t.Errorf("status = %v, want Error", span.Status().Code)
	}
	if len(span.Events()) == 0 {
		t.Error("the error was not recorded on the span")
	}
	var permanent bool
	for _, kv := range span.Attributes() {
		if kv.Key == "outbox.permanent" {
			permanent = kv.Value.AsBool()
		}
	}
	if !permanent {
		t.Error("a permanent failure was not marked as such")
	}
}

func TestTraceContextTravelsWithTheMessage(t *testing.T) {
	tr, rec, tp := newTracer(t)

	// An enqueueing request: its trace context is written into the headers.
	enqueueCtx, enqueueSpan := tp.Tracer("app").Start(context.Background(), "POST /orders")
	headers := outboxotel.InjectHeaders(enqueueCtx, nil)
	enqueueSpan.End()
	wantTrace := enqueueSpan.SpanContext().TraceID()

	if headers[outbox.HeaderTraceParent] == "" {
		t.Fatal("InjectHeaders wrote no traceparent")
	}

	// Later, in the worker, with no live parent in the context.
	m := outbox.Claimed{Message: outbox.Message{ID: outbox.NewID(), Type: "t", Headers: headers}, Attempt: 1}
	_, end := tr.StartPublish(context.Background(), &m)
	end(nil)

	var publish sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindProducer {
			publish = s
		}
	}
	if publish == nil {
		t.Fatal("no publish span was recorded")
	}
	if publish.SpanContext().TraceID() != wantTrace {
		t.Fatalf("publish span trace id = %s, want the enqueueing trace %s",
			publish.SpanContext().TraceID(), wantTrace)
	}
	if publish.Parent().SpanID() != enqueueSpan.SpanContext().SpanID() {
		t.Fatalf("publish span parent = %s, want the enqueueing span %s",
			publish.Parent().SpanID(), enqueueSpan.SpanContext().SpanID())
	}
	if !publish.Parent().IsRemote() {
		t.Error("the parent should be marked remote: it came from the message, not the call stack")
	}
}

func TestInjectHeadersIsSafeWithoutASpan(t *testing.T) {
	if got := outboxotel.InjectHeaders(context.Background(), nil); got != nil {
		t.Fatalf("InjectHeaders with no span = %v, want nil", got)
	}
	existing := map[string]string{"tenant": "acme"}
	got := outboxotel.InjectHeaders(context.Background(), existing)
	if len(got) != 1 || got["tenant"] != "acme" {
		t.Fatalf("InjectHeaders modified unrelated headers: %v", got)
	}
}

func TestMalformedTraceParentIsIgnored(t *testing.T) {
	tr, rec, _ := newTracer(t)
	m := outbox.Claimed{Message: outbox.Message{
		ID:      outbox.NewID(),
		Type:    "t",
		Headers: map[string]string{outbox.HeaderTraceParent: "garbage"},
	}}
	_, end := tr.StartPublish(context.Background(), &m)
	end(nil)

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if spans[0].Parent().IsValid() {
		t.Error("a malformed traceparent must not produce a parent")
	}
}

func TestLiveContextWinsOverHeaders(t *testing.T) {
	tr, rec, tp := newTracer(t)
	headers, err := outbox.InjectTraceParent(nil,
		"4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", true)
	if err != nil {
		t.Fatal(err)
	}
	liveCtx, live := tp.Tracer("app").Start(context.Background(), "live")
	defer live.End()

	m := outbox.Claimed{Message: outbox.Message{ID: outbox.NewID(), Type: "t", Headers: headers}}
	_, end := tr.StartPublish(liveCtx, &m)
	end(nil)

	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindProducer {
			if s.SpanContext().TraceID() != live.SpanContext().TraceID() {
				t.Fatalf("publish span joined the header trace, want the live one")
			}
			return
		}
	}
	t.Fatal("no publish span was recorded")
}

func TestTracerSatisfiesTheInterface(t *testing.T) {
	tr, _, _ := newTracer(t)
	var _ outbox.Tracer = tr
}
