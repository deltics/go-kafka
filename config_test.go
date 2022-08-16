package kafka

import (
	"context"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/deltics/go-kafka/hooks"
	"github.com/deltics/go-kafka/mock"
)

func Test_Config_copy(t *testing.T) {
	hooks := hooks.HookConsumer()
	middleware := func(*kafka.Message) (*kafka.Message, error) { return nil, nil }

	// ARRANGE
	cfg := NewConfig().
		WithHooks(hooks).
		WithMiddleware(middleware)

	t.Run("returns a copy of the config", func(t *testing.T) {
		copy := cfg.copy()

		if copy == cfg {
			t.Error("wanted a new copy of the config, got the same config")
		}
	})

	t.Run("copies hooks and middleware", func(t *testing.T) {
		copy := cfg.copy()
		{
			wanted := hooks
			got := copy.hooks
			if wanted != got {
				t.Error("wanted the original hooks, got a copy")
			}
		}
		if copy.middleware == nil {
			t.Error("wanted the original middleware, got nil")
		}
	})

	t.Run("copies config (map) and messageHandlers", func(t *testing.T) {
		ogmap := cfg.config
		oghandlers := cfg.messageHandlers

		copy := cfg.copy()

		wanted := reflect.ValueOf(ogmap).UnsafePointer()
		got := reflect.ValueOf(copy.config).UnsafePointer()
		if wanted == got {
			t.Error("got original config map, wanted a copy")
		}

		wanted = reflect.ValueOf(oghandlers).UnsafePointer()
		got = reflect.ValueOf(copy.messageHandlers).UnsafePointer()
		if wanted == got {
			t.Error("got original message handlers, wanted a copy")
		}
	})
}

func Test_Config_autoCommit(t *testing.T) {
	cfg := NewConfig()
	t.Run("returns setting of enable.auto.commit", func(t *testing.T) {
		cfg.config[key[enableAutoCommit]] = true
		wanted := true
		got := cfg.autoCommit()
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		cfg.config[key[enableAutoCommit]] = false
		wanted = false
		got = cfg.autoCommit()
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func Test_Config_With(t *testing.T) {
	cfg := NewConfig()
	copy := cfg.With("key", "value")

	t.Run("returns a copy of the config", func(t *testing.T) {
		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("adds the key and value to the config", func(t *testing.T) {
		wanted := "value"
		got, ok := copy.config["key"]
		if !ok || wanted != got {
			t.Errorf("wanted %q, got %q", wanted, got)
		}
	})
}

func Test_Config_WithAutoCommit(t *testing.T) {
	wanted := true
	cfg := NewConfig()
	copy := cfg.WithAutoCommit(wanted)

	t.Run("returns a copy of the config", func(t *testing.T) {
		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("sets enable.auto.commit", func(t *testing.T) {
		got, ok := copy.config[key[enableAutoCommit]].(bool)
		if !ok || wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func Test_Config_WithBatchSize(t *testing.T) {
	wanted := 10
	cfg := NewConfig()
	copy := cfg.WithBatchSize(wanted)

	t.Run("returns a copy of the config", func(t *testing.T) {
		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("sets batch.size", func(t *testing.T) {
		got, ok := copy.config[key[batchSize]].(int)
		if !ok || wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func Test_Config_WithBootstrapServers(t *testing.T) {
	// ARRANGE
	cfg := NewConfig()

	t.Run("returns a copy of the config", func(t *testing.T) {
		copy := cfg.WithBootstrapServers("")

		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("accepts an array of servers", func(t *testing.T) {
		cfg := cfg.WithBootstrapServers([]string{"server1", "server2"})

		wanted := "server1,server2"
		got := cfg.config[key[bootstrapServers]]
		if wanted != got {
			t.Errorf("wanted %q, got %q", wanted, got)
		}
	})

	t.Run("accepts a string", func(t *testing.T) {
		cfg := cfg.WithBootstrapServers("server1,server2")

		wanted := "server1,server2"
		got := cfg.config[key[bootstrapServers]]
		if wanted != got {
			t.Errorf("wanted %q, got %q", wanted, got)
		}
	})

	t.Run("panics if passed a []byte", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("did not panic")
			}
		}()

		cfg.WithBootstrapServers([]byte("server1,server2"))
	})
}

func Test_Config_WithHooks(t *testing.T) {
	wanted := mock.ConsumerHooks()
	cfg := NewConfig()
	copy := cfg.WithHooks(wanted)

	t.Run("returns a copy of the config", func(t *testing.T) {
		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("sets hooks", func(t *testing.T) {
		got := copy.hooks
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("panics if setting invalid hooks", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("did not panic")
			}
		}()
		cfg.WithHooks("this won't work")
	})
}

func Test_Config_WithMiddleware(t *testing.T) {
	middleware := func(*kafka.Message) (*kafka.Message, error) { return nil, nil }
	cfg := NewConfig()
	copy := cfg.WithMiddleware(middleware)

	t.Run("returns a copy of the config", func(t *testing.T) {
		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("sets middleware", func(t *testing.T) {
		wanted := reflect.ValueOf(middleware).Pointer()
		got := reflect.ValueOf(copy.middleware).Pointer()
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func Test_Config_WithNoClient(t *testing.T) {
	cfg := NewConfig()
	copy := cfg.WithNoClient()

	t.Run("returns a copy of the config", func(t *testing.T) {
		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("sets bootstrap.servers", func(t *testing.T) {
		wanted := "test://noclient"
		got := copy.config[key[bootstrapServers]]
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func Test_Config_WithGroupId(t *testing.T) {
	wanted := "group"
	cfg := NewConfig()
	copy := cfg.WithGroupId(wanted)

	t.Run("returns a copy of the config", func(t *testing.T) {
		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("sets group.id", func(t *testing.T) {
		got := copy.config[key[groupId]]
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func Test_Config_WithIdempotence(t *testing.T) {
	wanted := true
	cfg := NewConfig()
	copy := cfg.WithIdempotence(wanted)

	t.Run("returns a copy of the config", func(t *testing.T) {
		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("sets enable.idempotence", func(t *testing.T) {
		got := copy.config[key[enableIdempotence]]
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func Test_Config_WithMessageHandler(t *testing.T) {
	topic := "topic"
	handler := func(context.Context, *kafka.Message) error { return nil }
	cfg := NewConfig()
	copy := cfg.WithMessageHandler(topic, handler)

	t.Run("returns a copy of the config", func(t *testing.T) {
		if copy == cfg {
			t.Error("got the original, wanted a copy")
		}
	})

	t.Run("sets message handler for topic", func(t *testing.T) {
		wanted := reflect.ValueOf(handler).Pointer()
		got := reflect.ValueOf(copy.messageHandlers[topic]).Pointer()
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}
