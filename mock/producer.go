package mock

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/deltics/go-kafka/hooks"
)

type producerFuncs struct {
	Close        func(*kafka.Producer)
	Create       func(*kafka.ConfigMap) (*kafka.Producer, error)
	EventChannel func(*kafka.Producer) chan kafka.Event
	Flush        func(*kafka.Producer, int) int
	Produce      func(*kafka.Producer, *kafka.Message, chan kafka.Event) error
}

type producer struct {
	funcs  producerFuncs
	events chan kafka.Event
}

type MockProducerProvider interface {
	hooks.ProducerHooks
	Funcs() *producerFuncs
}

func ProducerHooks() MockProducerProvider {
	Events := make(chan kafka.Event)

	return &producer{
		events: Events,
		funcs: producerFuncs{
			Close:        func(*kafka.Producer) { close(Events) },
			Create:       func(*kafka.ConfigMap) (*kafka.Producer, error) { return &kafka.Producer{}, nil },
			EventChannel: func(*kafka.Producer) chan kafka.Event { return Events },
			Flush:        func(*kafka.Producer, int) int { return 0 },
			Produce:      func(*kafka.Producer, *kafka.Message, chan kafka.Event) error { return nil },
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

func (p *producer) GetEventChannel(producer *kafka.Producer) chan kafka.Event {
	return p.funcs.EventChannel(producer)
}

func (p *producer) Flush(producer *kafka.Producer, timeoutMs int) int {
	return p.funcs.Flush(producer, timeoutMs)
}

func (p *producer) Produce(producer *kafka.Producer, msg *kafka.Message, ch chan kafka.Event) error {
	return p.funcs.Produce(producer, msg, ch)
}
