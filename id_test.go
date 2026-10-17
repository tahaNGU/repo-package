package outbox

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewIDIsVersion7AndUnique(t *testing.T) {
	seen := make(map[ID]bool, 1000)
	before := time.Now().Add(-2 * time.Second)
	for range 1000 {
		id := NewID()
		if id.IsZero() {
			t.Fatal("NewID returned the zero id")
		}
		if seen[id] {
			t.Fatalf("NewID returned a duplicate: %s", id)
		}
		seen[id] = true
		if got := id[6] >> 4; got != 7 {
			t.Fatalf("version = %d, want 7", got)
		}
		if got := id[8] >> 6; got != 0b10 {
			t.Fatalf("variant bits = %b, want 10", got)
		}
		ts, ok := id.Time()
		if !ok {
			t.Fatal("Time() reported no timestamp for a v7 id")
		}
		if ts.Before(before) || ts.After(time.Now().Add(time.Second)) {
			t.Fatalf("embedded timestamp %s is out of range", ts)
		}
	}
}

func TestIDRoundTrip(t *testing.T) {
	id := NewID()
	parsed, err := ParseID(id.String())
	if err != nil {
		t.Fatalf("ParseID: %v", err)
	}
	if parsed != id {
		t.Fatalf("round trip changed the id: %s != %s", parsed, id)
	}

	var scanned ID
	if err := scanned.Scan(id.String()); err != nil {
		t.Fatalf("Scan(string): %v", err)
	}
	if scanned != id {
		t.Fatal("Scan(string) produced a different id")
	}
	scanned = ID{}
	if err := scanned.Scan(id[:]); err != nil {
		t.Fatalf("Scan([]byte): %v", err)
	}
	if scanned != id {
		t.Fatal("Scan(16 bytes) produced a different id")
	}
	v, err := id.Value()
	if err != nil || v != id.String() {
		t.Fatalf("Value() = %v, %v", v, err)
	}

	b, err := json.Marshal(struct{ ID ID }{id})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out struct{ ID ID }
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.ID != id {
		t.Fatal("JSON round trip changed the id")
	}
}

func TestParseIDRejectsMalformed(t *testing.T) {
	for _, s := range []string{
		"",
		"not-a-uuid",
		"0191b5f2-7c5a-7c2a-8e4a-1f2b3c4d5e6",   // too short
		"0191b5f2-7c5a-7c2a-8e4a-1f2b3c4d5e6f7", // too long
		"0191b5f27c5a7c2a8e4a1f2b3c4d5e6f",      // no dashes
		"0191b5f2-7c5a-7c2a-8e4a-1f2b3c4d5exf",  // bad hex
	} {
		if _, err := ParseID(s); err == nil {
			t.Errorf("ParseID(%q) accepted malformed input", s)
		}
	}
	if err := new(ID).Scan(42); err == nil {
		t.Error("Scan accepted an int")
	}
	var id ID
	if err := id.Scan(nil); err != nil || !id.IsZero() {
		t.Errorf("Scan(nil) = %v, id %s; want no error and zero id", err, id)
	}
}

func TestIDTimeRejectsNonV7(t *testing.T) {
	id := MustParseID("0191b5f2-7c5a-4c2a-8e4a-1f2b3c4d5e6f") // version 4
	if _, ok := id.Time(); ok {
		t.Fatal("Time() claimed a timestamp for a v4 id")
	}
}
