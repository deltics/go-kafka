package api

import "github.com/confluentinc/confluent-kafka-go/kafka"

type ProducerHooks struct {
	Close   func(*kafka.Producer)
	Create  func(*kafka.ConfigMap) (*kafka.Producer, error)
	Events  func(*kafka.Producer) chan kafka.Event
	Produce func(*kafka.Producer, *kafka.Message, chan kafka.Event) error
}

var Producer = ProducerHooks{
	Close:   closeProducer,
	Create:  createProducer,
	Events:  producerEvents,
	Produce: produceMessage,
}

func closeProducer(p *kafka.Producer) {
	p.Close()
}

func createProducer(cfg *kafka.ConfigMap) (*kafka.Producer, error) {
	return kafka.NewProducer(cfg)
}

func producerEvents(p *kafka.Producer) chan kafka.Event {
	return p.Events()
}

func produceMessage(p *kafka.Producer, m *kafka.Message, c chan kafka.Event) error {
	return p.Produce(m, c)
}
