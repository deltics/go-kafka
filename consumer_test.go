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
	c.Run(context.Background())

	// ASSERT
	if !closed {
		t.Error("consumer was not closed")
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
		WithMessageHandler("topicA", func(ctx context.Context, msg *kafka.Message) error { return nil }).
		WithMessageHandler("topicB", func(ctx context.Context, msg *kafka.Message) error { return nil })

	c, _ := NewConsumer(cfg)

	// ACT
	c.Run(context.Background())

	// ASSERT
	if len(topics) != 2 {
		t.Errorf("expected %d topics to be subscribed, got %d", 2, len(topics))
	}
	if topics[0] != "topicA" && topics[1] != "topicA" {
		t.Error("consumer did not subscribe to topicA")
	}
	if topics[0] != "topicB" && topics[1] != "topicB" {
		t.Error("consumer did not subscribe to topicB")
	}
}

func TestThatTheConsumerDispatchesMessagesToTheCorrectTopicHandler(t *testing.T) {
	// MOCK
	received := map[string]string{}

	p := mock.ConsumerHooks()
	p.Messages([]interface{}{
		StringMessage("topicA", "message for A"),
		StringMessage("topicB", "message for B"),
	})

	// ARRANGE
	cfg := NewConfig().WithHooks(p).
		WithMessageHandler("topicA", func(ctx context.Context, msg *kafka.Message) error {
			received["topicA"] = string(msg.Value)
			return nil
		}).
		WithMessageHandler("topicB", func(ctx context.Context, msg *kafka.Message) error {
			received["topicB"] = string(msg.Value)
			return nil
		})

	c, _ := NewConsumer(cfg)

	// ACT
	c.Run(context.Background())

	// ASSERT
	if len(received) != 2 {
		t.Errorf("expected %d topic handlers to receive messages, got %d", 2, len(received))
	}
	if received["topicA"] != "message for A" {
		t.Errorf("expected handler for %v would receive %v, got %v", "topicA", "message for A", received["topicA"])
	}
	if received["topicB"] != "message for B" {
		t.Errorf("expected handler for %v would receive %v, got %v", "topicB", "message for B", received["topicB"])
	}
}

func TestThatConsumerMiddlewareIsCalled(t *testing.T) {
	// MOCK
	topicId := "topicA"
	message := "test message"
	var middlewareReceived string

	p := mock.ConsumerHooks()
	p.Messages([]interface{}{
		StringMessage(topicId, message),
	})

	// ARRANGE
	cfg := NewConfig().WithHooks(p).
		WithMiddleware(func(msg *kafka.Message) (*kafka.Message, error) {
			middlewareReceived = string(msg.Value)
			return msg, nil
		}).
		WithMessageHandler(topicId, func(ctx context.Context, msg *kafka.Message) error { return nil })

	c, _ := NewConsumer(cfg)

	// ACT
	c.Run(context.Background())

	// ASSERT
	if middlewareReceived != message {
		t.Error("middleware was not called")
	}
}
