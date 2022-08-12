package kafka

import "testing"

func TestThatFluentConfigurationReturnsCopies(t *testing.T) {
	// ARRANGE
	og := NewConfig().WithBootstrapServers("og server")

	// ACT
	cc := og.WithBootstrapServers("cc server")

	// ASSERT
	if og == cc {
		t.Error("wanted a new copy of the config, got a reference to the same config")
	}

	wanted := "og server"
	got := og.cm[key[bootstrapServers]]
	if wanted != got {
		t.Errorf("wanted bootstrapServers %q, got %q", wanted, got)
	}
}

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
