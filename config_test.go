package kafka

import "testing"

func TestThatConfigEnforcesMaxProducerRetryConstraints(t *testing.T) {
	// ARRANGE
	cfg := NewConfig()

	// ACT
	low := cfg.WithMaxProducerRetries(-1)
	high := cfg.WithMaxProducerRetries(6)
	middle := cfg.WithMaxProducerRetries(3)

	// ASSERT
	if low.maxProducerRetries < 0 {
		t.Errorf("Wanted maxProducerRetries == 0, got %d", low.maxProducerRetries)
	}
	if high.maxProducerRetries > 5 {
		t.Errorf("Wanted maxProducerRetries == 5, got %d", high.maxProducerRetries)
	}
	if middle.maxProducerRetries != 3 {
		t.Errorf("Wanted maxProducerRetries == 3, got %d", middle.maxProducerRetries)
	}
}
