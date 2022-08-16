package hooks

import "github.com/confluentinc/confluent-kafka-go/kafka"

type ProducerHooks interface {
	Close(*kafka.Producer)
	Create(*kafka.ConfigMap) (*kafka.Producer, error)
	GetEventChannel(*kafka.Producer) chan kafka.Event
	Flush(*kafka.Producer, int) int
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
	bss, _ := cfg.Get("bootstrap.servers", "")
	if bss == "test://noclient" {
		return &kafka.Producer{}, nil
	}
	return kafka.NewProducer(cfg)
}

func (*producer) GetEventChannel(producer *kafka.Producer) chan kafka.Event {
	return producer.Events()
}

func (*producer) Flush(producer *kafka.Producer, timeoutMs int) int {
	return producer.Flush(timeoutMs)
}

func (*producer) Produce(producer *kafka.Producer, msg *kafka.Message, ch chan kafka.Event) error {
	return producer.Produce(msg, ch)
}
