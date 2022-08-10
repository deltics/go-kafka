package mock

import (
	"kafka/api"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var Consumer = api.ConsumerHooks{
	Create:        func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) { return &kafka.Consumer{}, nil },
	Close:         func(c *kafka.Consumer) {},
	CommitOffset:  func(c *kafka.Consumer, tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error) { return tpa, nil },
	MessageReader: &noMessages{},
	Subscribe:     func(c *kafka.Consumer, ta []string, rcb kafka.RebalanceCb) error { return nil },
}
