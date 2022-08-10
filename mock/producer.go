package mock

import (
	"kafka/api"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var Producer = api.ProducerHooks{
	Close:   func(p *kafka.Producer) {},
	Create:  func(cfg *kafka.ConfigMap) (*kafka.Producer, error) { return &kafka.Producer{}, nil },
	Events:  func(p *kafka.Producer) chan kafka.Event { return nil },
	Produce: func(p *kafka.Producer, m *kafka.Message, c chan kafka.Event) error { return nil },
}
