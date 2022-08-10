package mock

import (
	"errors"
	"fmt"
	"kafka/hooks"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type consumerFuncs struct {
	Create       func(cfg *kafka.ConfigMap) (*kafka.Consumer, error)
	Close        func(c *kafka.Consumer)
	CommitOffset func(c *kafka.Consumer, tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	Subscribe    func(c *kafka.Consumer, ta []string, rcb kafka.RebalanceCb) error
}

type consumerHooks interface {
	hooks.ConsumerHooks
	Funcs() *consumerFuncs
	Messages([]interface{})
}

type consumer struct {
	funcs        consumerFuncs
	messages     []interface{}
	messageIndex int
}

func ConsumerHooks() consumerHooks {
	return &consumer{
		messages: []interface{}{},
		funcs: consumerFuncs{
			Create:       func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) { return &kafka.Consumer{}, nil },
			Close:        func(c *kafka.Consumer) {},
			CommitOffset: func(c *kafka.Consumer, tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error) { return tpa, nil },
			Subscribe:    func(c *kafka.Consumer, ta []string, rcb kafka.RebalanceCb) error { return nil },
		},
	}
}

func (c *consumer) Funcs() *consumerFuncs {
	return &c.funcs
}

func (c *consumer) Messages(msgs []interface{}) {
	c.messages = append(c.messages, msgs...)
}

func (c *consumer) Create(cfg *kafka.ConfigMap) (*kafka.Consumer, error) {
	return c.funcs.Create(cfg)
}

func (c *consumer) Close(consumer *kafka.Consumer) {
	c.funcs.Close(consumer)
}

func (c *consumer) CommitOffset(consumer *kafka.Consumer, partition []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return c.funcs.CommitOffset(consumer, partition)
}

func (c *consumer) Subscribe(consumer *kafka.Consumer, topics []string, rebalanceCallback kafka.RebalanceCb) error {
	return c.funcs.Subscribe(consumer, topics, rebalanceCallback)
}

func (c *consumer) ReadMessage(consumer *kafka.Consumer, timeout time.Duration) (*kafka.Message, error) {
	if c.messageIndex >= len(c.messages) {
		return nil, errors.New("no more messages")
	}

	msg := c.messages[c.messageIndex]
	c.messageIndex++

	switch msg := msg.(type) {
	case *kafka.Message:
		return msg, nil
	case error:
		return nil, msg
	}
	return nil, fmt.Errorf("unexpected item of type %T in mock message list", msg)
}
