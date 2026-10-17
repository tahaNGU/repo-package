package outbox

import (
	"context"
	"fmt"
	"strings"
)

// Tracer creates one span per delivery attempt. It is optional and lives
// behind an interface so the core package does not depend on an
// instrumentation SDK; see github.com/tahaNGU/gocast/contrib/otel for an
// OpenTelemetry implementation.
//
// A message is usually published long after the transaction that produced it,
// in a different goroutine and possibly a different process, so there is no
// live parent span to attach to. The link instead travels with the message:
// write a W3C trace context into Headers[HeaderTraceParent] at enqueue time
// (see [InjectTraceParent]) and the Tracer implementation can use it as the
// remote parent of the publish span.
type Tracer interface {
	// StartPublish returns a context carrying the span and a function to end
	// it. The returned context is passed to Publisher.Publish, so a publisher
	// that is itself instrumented will nest correctly.
	StartPublish(ctx context.Context, m *Claimed) (context.Context, EndSpan)
}

// EndSpan completes a span started by a [Tracer]. err is nil on success.
type EndSpan func(err error)

// InjectTraceParent stores a W3C "traceparent" value in headers, allocating
// the map if needed, so the eventual publish span can be linked to the
// transaction that enqueued the message. traceID must be 32 hex characters and
// spanID 16; sampled sets the sampled flag.
func InjectTraceParent(headers map[string]string, traceID, spanID string, sampled bool) (map[string]string, error) {
	if len(traceID) != 32 || len(spanID) != 16 {
		return headers, fmt.Errorf("outbox: invalid trace ids (want 32 and 16 hex chars, got %d and %d)", len(traceID), len(spanID))
	}
	flags := "00"
	if sampled {
		flags = "01"
	}
	if headers == nil {
		headers = make(map[string]string, 1)
	}
	headers[HeaderTraceParent] = "00-" + strings.ToLower(traceID) + "-" + strings.ToLower(spanID) + "-" + flags
	return headers, nil
}

// TraceParent reads the W3C trace context written by [InjectTraceParent].
// ok is false when the header is missing or malformed.
func TraceParent(headers map[string]string) (traceID, spanID string, sampled, ok bool) {
	v, found := headers[HeaderTraceParent]
	if !found {
		return "", "", false, false
	}
	parts := strings.Split(v, "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return "", "", false, false
	}
	return parts[1], parts[2], parts[3] != "00", true
}
