package mock

import (
	"kafka/hooks"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type producerFuncs struct {
	Close   func(p *kafka.Producer)
	Create  func(cfg *kafka.ConfigMap) (*kafka.Producer, error)
	Events  func(p *kafka.Producer) chan kafka.Event
	Produce func(p *kafka.Producer, m *kafka.Message, c chan kafka.Event) error
}

type producer struct {
	funcs producerFuncs
}

type MockProducerProvider interface {
	hooks.ProducerHooks
	Funcs() *producerFuncs
}

func MockProducer() MockProducerProvider {
	return &producer{
		funcs: producerFuncs{
			Close:   func(p *kafka.Producer) {},
			Create:  func(cfg *kafka.ConfigMap) (*kafka.Producer, error) { return &kafka.Producer{}, nil },
			Events:  func(p *kafka.Producer) chan kafka.Event { return nil },
			Produce: func(p *kafka.Producer, m *kafka.Message, c chan kafka.Event) error { return nil },
		},
	}
}

func (p *producer) Funcs() *producerFuncs {
	return &p.funcs
}

func (p *producer) Close(producer *kafka.Producer) {
	p.funcs.Close(producer)
}

func (p *producer) Create(cfg *kafka.ConfigMap) (*kafka.Producer, error) {
	return p.funcs.Create(cfg)
}

func (p *producer) Events(producer *kafka.Producer) chan kafka.Event {
	return p.funcs.Events(producer)
}

func (p *producer) Produce(producer *kafka.Producer, msg *kafka.Message, ch chan kafka.Event) error {
	return p.funcs.Produce(producer, msg, ch)
}
