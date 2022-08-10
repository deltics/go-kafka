package hooks

import "github.com/confluentinc/confluent-kafka-go/kafka"

type ProducerHooks interface {
	Close(*kafka.Producer)
	Create(*kafka.ConfigMap) (*kafka.Producer, error)
	Events(*kafka.Producer) chan kafka.Event
	Produce(*kafka.Producer, *kafka.Message, chan kafka.Event) error
}

type producer struct{}

func HookProducer() ProducerHooks {
	return &producer{}
}

func (*producer) Close(producer *kafka.Producer) {
	producer.Close()
}

func (*producer) Create(cfg *kafka.ConfigMap) (*kafka.Producer, error) {
	return kafka.NewProducer(cfg)
}

func (*producer) Events(producer *kafka.Producer) chan kafka.Event {
	return producer.Events()
}

func (*producer) Produce(producer *kafka.Producer, msg *kafka.Message, ch chan kafka.Event) error {
	return producer.Produce(msg, ch)
}
