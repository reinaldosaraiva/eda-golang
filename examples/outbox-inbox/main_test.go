package main

import (
	"encoding/json"
	"testing"
)

// The envelope is the wire contract: the idempotency key must travel in the
// payload and survive a marshal/unmarshal round-trip untouched.
func TestEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()

	original := Envelope{
		ID:      "7d3c9f20-0000-4000-8000-000000000042",
		Name:    "ordersapi.OrderPlaced",
		Payload: OrderPlaced{OrderID: "order-7", Amount: 99.90},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded Envelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded != original {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", decoded, original)
	}
}

// A missing or empty envelope ID breaks inbox dedup silently — guard the
// contract so it fails loud instead.
func TestEnvelopeIDIsRequired(t *testing.T) {
	t.Parallel()

	env := Envelope{Name: "ordersapi.OrderPlaced"}
	if env.ID != "" {
		t.Fatal("precondition: zero-value envelope must have empty ID")
	}
	// The consumer must reject rather than dedup everything under the same
	// empty key — see validateEnvelope.
	if err := validateEnvelope(env); err == nil {
		t.Error("validateEnvelope: expected error for empty ID, got nil")
	}
}

func TestValidateEnvelopeAcceptsValid(t *testing.T) {
	t.Parallel()

	env := Envelope{ID: "abc-123", Name: "ordersapi.OrderPlaced"}
	if err := validateEnvelope(env); err != nil {
		t.Errorf("validateEnvelope: unexpected error: %v", err)
	}
}

func TestEnvOr(t *testing.T) {
	if got := envOr("EDA_TEST_UNSET_VAR", "fallback"); got != "fallback" {
		t.Errorf("envOr unset = %q, want fallback", got)
	}
	t.Setenv("EDA_TEST_SET_VAR", "value")
	if got := envOr("EDA_TEST_SET_VAR", "fallback"); got != "value" {
		t.Errorf("envOr set = %q, want value", got)
	}
}
