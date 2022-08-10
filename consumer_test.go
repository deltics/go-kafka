package kafka

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"kafka/mock"
)

func TestThatTheConsumerIsClosedWhenRunTerminates(t *testing.T) {
	// SPY
	closed := false

	// ARRANGE
	c, _ := NewConsumer(NewConfig().WithMockApi())
	c.api.Close = func(c *kafka.Consumer) { closed = true }

	// ACT
	c.Run()

	// ASSERT
	if !closed {
		t.Error("Consumer was not closed")
	}
}

func TestThatTheConsumerSubscribesToTopicsWithHandlers(t *testing.T) {
	// SPY
	topics := []string{}

	// ARRANGE
	cfg := NewConfig().WithMockApi().
		WithTopicHandler("topicA", func(ctx context.Context, msg []byte) error { return nil }).
		WithTopicHandler("topicB", func(ctx context.Context, msg []byte) error { return nil })

	c, _ := NewConsumer(cfg)
	c.api.Subscribe = func(c *kafka.Consumer, ta []string, rcb kafka.RebalanceCb) error {
		topics = append(topics, ta...)
		return nil
	}

	// ACT
	c.Run()

	// ASSERT
	if len(topics) != 2 {
		t.Errorf("Expected %d topics to be subscribed, got %d", 2, len(topics))
	}
	if topics[0] != "topicA" && topics[1] != "topicA" {
		t.Error("Consumer did not subscribe to topicA")
	}
	if topics[0] != "topicB" && topics[1] != "topicB" {
		t.Error("Consumer did not subscribe to topicB")
	}
}

func TestThatTheConsumerDispatchesMessagesToTheCorrectTopicHandler(t *testing.T) {
	// SPY
	received := map[string]string{}

	// ARRANGE
	cfg := NewConfig().WithMockApi().
		WithTopicHandler("topicA", func(ctx context.Context, msg []byte) error {
			received["topicA"] = string(msg)
			return nil
		}).
		WithTopicHandler("topicB", func(ctx context.Context, msg []byte) error {
			received["topicB"] = string(msg)
			return nil
		})

	c, _ := NewConsumer(cfg)
	c.api.MessageReader = mock.Messages([]interface{}{
		mock.MessageWithString("topicA", "message for A"),
		mock.MessageWithString("topicB", "message for B"),
	})

	// ACT
	c.Run()

	// ASSERT
	if len(received) != 2 {
		t.Errorf("Expected %d topic handlers to receive messages, got %d", 2, len(received))
	}
	if received["topicA"] != "message for A" {
		t.Errorf("Expected handler for %v would receive %v, got %v", "topicA", "message for A", received["topicA"])
	}
	if received["topicB"] != "message for B" {
		t.Errorf("Expected handler for %v would receive %v, got %v", "topicB", "message for B", received["topicB"])
	}
}

func TestThatConsumerMiddlewareIsCalled(t *testing.T) {
	// ARRANGE
	topicId := "topicA"
	message := "test message"
	var middlewareReceived string

	cfg := NewConfig().WithMockApi().
		WithMiddleware(func(bytes []byte) ([]byte, error) {
			middlewareReceived = string(bytes)
			return bytes, nil
		}).
		WithTopicHandler(topicId, func(ctx context.Context, msg []byte) error { return nil })

	c, _ := NewConsumer(cfg)

	c.api.MessageReader = mock.Messages([]interface{}{
		mock.MessageWithString(topicId, message),
	})

	// ACT
	c.Run()

	// ASSERT
	if middlewareReceived != message {
		t.Error("Middleware was not called")
	}
}

// func Test(t *testing.T) {
// 	// ARRANGE
// 	c := NewConsumer(NewConfig(ConfigMap{}))

// 	// ACT

// 	// ASSERT
// 	if  {
// 		t.Error("")
// 	}
// }
