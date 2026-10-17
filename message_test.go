package outbox

import (
	"errors"
	"strings"
	"testing"
)

func TestMessageValidate(t *testing.T) {
	if err := (Message{Type: "orders.created.v1"}).Validate(); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}
	if err := (Message{Type: "t", Payload: nil}).Validate(); err != nil {
		t.Fatalf("empty payload must be allowed: %v", err)
	}

	bad := map[string]Message{
		"missing type":      {},
		"type too long":     {Type: strings.Repeat("x", MaxTypeLen+1)},
		"empty header name": {Type: "t", Headers: map[string]string{"": "v"}},
		"oversized header":  {Type: "t", Headers: map[string]string{"k": strings.Repeat("v", 5000)}},
	}
	for name, m := range bad {
		err := m.Validate()
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("%s: error %v does not wrap ErrInvalidMessage", name, err)
		}
	}
}

func TestClassifyAndPermanent(t *testing.T) {
	base := errors.New("broker said no")
	if Classify(base) != Retryable {
		t.Error("plain errors must be retryable")
	}
	p := Permanent(base)
	if Classify(p) != PermanentFailure {
		t.Error("wrapped errors must be permanent")
	}
	if !IsPermanent(p) {
		t.Error("IsPermanent should recognise its own wrapper")
	}
	if !errors.Is(p, base) {
		t.Error("Permanent must preserve the cause")
	}
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) must be nil")
	}
	if IsPermanent(base) {
		t.Error("plain errors must not look permanent")
	}
	wrapped := errors.Join(errors.New("other"), Permanent(base))
	if !IsPermanent(wrapped) {
		t.Error("IsPermanent must see through error trees")
	}
	if got := (&PermanentError{}).Error(); got == "" {
		t.Error("PermanentError with no cause must still have a message")
	}
}

func TestTruncatePreservesRunes(t *testing.T) {
	if got := truncate("hello", 10); got != "hello" {
		t.Fatalf("truncate shortened a short string: %q", got)
	}
	got := truncate("héllo wörld", 3)
	if len(got) > 3+len("…") {
		t.Fatalf("truncate produced %q, too long", got)
	}
	// "é" is two bytes at offsets 1-2; cutting at 2 must step back to 1.
	if truncate("héllo", 2) != "h…" {
		t.Fatalf("truncate split a rune: %q", truncate("héllo", 2))
	}
}

func TestDefaultErrorSummaryBounded(t *testing.T) {
	if defaultErrorSummary(nil) != "" {
		t.Error("nil error must summarise to the empty string")
	}
	long := errors.New(strings.Repeat("x", 10_000))
	if got := defaultErrorSummary(long); len(got) > 512+len("…") {
		t.Fatalf("summary is %d bytes, want <= 512", len(got))
	}
}

func TestTraceParentRoundTrip(t *testing.T) {
	const traceID, spanID = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	h, err := InjectTraceParent(nil, traceID, spanID, true)
	if err != nil {
		t.Fatalf("InjectTraceParent: %v", err)
	}
	if h[HeaderTraceParent] != "00-"+traceID+"-"+spanID+"-01" {
		t.Fatalf("traceparent = %q", h[HeaderTraceParent])
	}
	gotTrace, gotSpan, sampled, ok := TraceParent(h)
	if !ok || gotTrace != traceID || gotSpan != spanID || !sampled {
		t.Fatalf("TraceParent = %q, %q, %v, %v", gotTrace, gotSpan, sampled, ok)
	}
	if _, err := InjectTraceParent(nil, "short", spanID, false); err == nil {
		t.Error("InjectTraceParent accepted a malformed trace id")
	}
	for _, bad := range []string{"", "garbage", "01-x-y-z", "00-" + traceID + "-" + spanID} {
		if _, _, _, ok := TraceParent(map[string]string{HeaderTraceParent: bad}); ok {
			t.Errorf("TraceParent accepted %q", bad)
		}
	}
	if _, _, _, ok := TraceParent(nil); ok {
		t.Error("TraceParent accepted a missing header")
	}
}
