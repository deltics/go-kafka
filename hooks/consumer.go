package hooks

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConsumerHooks interface {
	Create(*kafka.ConfigMap) (*kafka.Consumer, error)
	Close(*kafka.Consumer)
	CommitOffset(*kafka.Consumer, []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	ReadMessage(*kafka.Consumer, time.Duration) (*kafka.Message, error)
	Subscribe(c *kafka.Consumer, ta []string, rcb kafka.RebalanceCb) error
}

type consumer struct{}

func HookConsumer() ConsumerHooks {
	return &consumer{}
}

func (*consumer) Close(c *kafka.Consumer) {
	c.Close()
}

func (*consumer) CommitOffset(c *kafka.Consumer, tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return c.CommitOffsets(tpa)
}

func (*consumer) Create(cfg *kafka.ConfigMap) (*kafka.Consumer, error) {
	return kafka.NewConsumer(cfg)
}

func (*consumer) Subscribe(c *kafka.Consumer, ta []string, rcb kafka.RebalanceCb) error {
	return c.SubscribeTopics(ta, rcb)
}

func (*consumer) ReadMessage(c *kafka.Consumer, t time.Duration) (*kafka.Message, error) {
	return c.ReadMessage(t)
}
