package task

import (
	"testing"
	"time"
)

func TestTaskCreationAndOptions(t *testing.T) {
	payload := []byte(`{"order_id":12345}`)
	future := time.Now().UTC().Add(10 * time.Minute)

	tsk := NewTask(
		"create_invoice",
		payload,
		WithQueue("billing"),
		WithID("inv-12345"),
		WithPriority(PriorityCritical),
		WithMaxRetries(5),
		WithTimeout(45*time.Second),
		WithVisibilityTimeout(90*time.Second),
		WithScheduleAt(future),
	)

	if tsk.ID != "inv-12345" {
		t.Errorf("expected ID inv-12345, got %s", tsk.ID)
	}
	if tsk.Queue != "billing" {
		t.Errorf("expected Queue billing, got %s", tsk.Queue)
	}
	if tsk.Priority != PriorityCritical {
		t.Errorf("expected Priority Critical, got %s", tsk.Priority)
	}
	if tsk.MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", tsk.MaxRetries)
	}
	if tsk.Timeout != 45*time.Second {
		t.Errorf("expected Timeout 45s, got %v", tsk.Timeout)
	}
	if tsk.VisibilityTimeout != 90*time.Second {
		t.Errorf("expected VisibilityTimeout 90s, got %v", tsk.VisibilityTimeout)
	}

	// Test Serialization roundtrip
	data, err := tsk.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal task: %v", err)
	}

	deserialized, err := UnmarshalTask(data)
	if err != nil {
		t.Fatalf("failed to unmarshal task: %v", err)
	}

	if deserialized.ID != tsk.ID || deserialized.Type != tsk.Type || deserialized.Queue != tsk.Queue {
		t.Errorf("deserialization mismatch: %+v vs %+v", deserialized, tsk)
	}
}

func TestRetryPolicyBackoffWithJitter(t *testing.T) {
	policy := NewRetryPolicy(100*time.Millisecond, 2*time.Second, 2.0)

	// Attempt 1: backoff is between 50ms and 100ms
	for i := 0; i < 20; i++ {
		backoff := policy.CalculateBackoff(1)
		if backoff < 50*time.Millisecond || backoff > 100*time.Millisecond {
			t.Errorf("attempt 1 backoff out of expected bounds [50ms, 100ms]: %v", backoff)
		}
	}

	// Attempt 5: should be bounded by MaxDelay (2s)
	for i := 0; i < 20; i++ {
		backoff := policy.CalculateBackoff(5)
		if backoff > 2*time.Second {
			t.Errorf("attempt 5 backoff exceeded MaxDelay: %v", backoff)
		}
		if backoff < 50*time.Millisecond {
			t.Errorf("attempt 5 backoff below min delay: %v", backoff)
		}
	}
}

func TestPriorityOrderingAndLevels(t *testing.T) {
	if PriorityCritical.Level() <= PriorityHigh.Level() {
		t.Errorf("Critical level should be > High level")
	}
	if PriorityHigh.Level() <= PriorityDefault.Level() {
		t.Errorf("High level should be > Default level")
	}
	if PriorityDefault.Level() <= PriorityLow.Level() {
		t.Errorf("Default level should be > Low level")
	}

	if !PriorityCritical.IsValid() || !PriorityHigh.IsValid() || !PriorityDefault.IsValid() || !PriorityLow.IsValid() {
		t.Errorf("all standard priorities should be valid")
	}
	if Priority("unknown").IsValid() {
		t.Errorf("unknown priority should not be valid")
	}
}
