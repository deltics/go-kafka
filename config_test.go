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
	got := og.config[key[bootstrapServers]]
	if wanted != got {
		t.Errorf("wanted bootstrapServers %q, got %q", wanted, got)
	}
}
