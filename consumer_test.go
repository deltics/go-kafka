package kafka

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/deltics/go-kafka/mock"
)

func TestThatTheConsumerIsClosedWhenRunTerminates(t *testing.T) {
	// MOCK
	closed := false

	p := mock.ConsumerHooks()
	p.Funcs().Close = func(c *kafka.Consumer) { closed = true }

	// ARRANGE
	cfg := NewConfig().WithHooks(p)
	c, _ := NewConsumer(cfg)

	// ACT
	c.Run()

	// ASSERT
	if !closed {
		t.Error("Consumer was not closed")
	}
}

func TestThatTheConsumerSubscribesToTopicsWithHandlers(t *testing.T) {
	// MOCK
	topics := []string{}

	p := mock.ConsumerHooks()
	p.Funcs().Subscribe = func(c *kafka.Consumer, ta []string, rcb kafka.RebalanceCb) error {
		topics = append(topics, ta...)
		return nil
	}

	// ARRANGE
	cfg := NewConfig().WithHooks(p).
		WithTopicHandler("topicA", func(ctx context.Context, msg []byte) error { return nil }).
		WithTopicHandler("topicB", func(ctx context.Context, msg []byte) error { return nil })

	c, _ := NewConsumer(cfg)

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
	// MOCK
	received := map[string]string{}

	p := mock.ConsumerHooks()
	p.Messages([]interface{}{
		mock.StringMessage("topicA", "message for A"),
		mock.StringMessage("topicB", "message for B"),
	})

	// ARRANGE
	cfg := NewConfig().WithHooks(p).
		WithTopicHandler("topicA", func(ctx context.Context, msg []byte) error {
			received["topicA"] = string(msg)
			return nil
		}).
		WithTopicHandler("topicB", func(ctx context.Context, msg []byte) error {
			received["topicB"] = string(msg)
			return nil
		})

	c, _ := NewConsumer(cfg)

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
	// MOCK
	topicId := "topicA"
	message := "test message"
	var middlewareReceived string

	p := mock.ConsumerHooks()
	p.Messages([]interface{}{
		mock.StringMessage(topicId, message),
	})

	// ARRANGE
	cfg := NewConfig().WithHooks(p).
		WithMiddleware(func(bytes []byte) ([]byte, error) {
			middlewareReceived = string(bytes)
			return bytes, nil
		}).
		WithTopicHandler(topicId, func(ctx context.Context, msg []byte) error { return nil })

	c, _ := NewConsumer(cfg)

	// ACT
	c.Run()

	// ASSERT
	if middlewareReceived != message {
		t.Error("Middleware was not called")
	}
}
