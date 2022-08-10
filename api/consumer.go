package api

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MessageReader interface {
	ReadMessage(*kafka.Consumer, time.Duration) (*kafka.Message, error)
}

type ConsumerHooks struct {
	Create        func(*kafka.ConfigMap) (*kafka.Consumer, error)
	Close         func(*kafka.Consumer)
	CommitOffset  func(*kafka.Consumer, []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	MessageReader MessageReader
	Subscribe     func(c *kafka.Consumer, ta []string, rcb kafka.RebalanceCb) error
}

var Consumer = ConsumerHooks{
	Create:        createConsumer,
	Close:         closeConsumer,
	CommitOffset:  commitOffset,
	MessageReader: &messageReader{},
	Subscribe:     subscribe,
}

func closeConsumer(c *kafka.Consumer) {
	c.Close()
}

func commitOffset(c *kafka.Consumer, tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return c.CommitOffsets(tpa)
}

func createConsumer(cfg *kafka.ConfigMap) (*kafka.Consumer, error) {
	return kafka.NewConsumer(cfg)
}

func subscribe(c *kafka.Consumer, ta []string, rcb kafka.RebalanceCb) error {
	return c.SubscribeTopics(ta, rcb)
}

type messageReader struct{}

func (*messageReader) ReadMessage(c *kafka.Consumer, t time.Duration) (*kafka.Message, error) {
	return c.ReadMessage(t)
}
