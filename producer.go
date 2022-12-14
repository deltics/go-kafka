package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/deltics/go-kafka/hooks"
)

type ErrTimeOut struct{}

func (e ErrTimeOut) Error() string { return "timed out" }

type producer struct {
	hooks          hooks.ProducerHooks
	config         *config
	producer       *kafka.Producer
	middleware     MessageMiddleware
	DeliveryEvents chan kafka.Event
}

type ProducerEventHandler interface {
	OnMessageDelivered(*kafka.Message)
	OnMessageError(*kafka.Message, error)
	OnProducerError(kafka.Error, *bool)
	OnUnexpectedEvent(kafka.Event, *bool)
}

func NewProducer(cfg *config) (*producer, error) {
	phk, ok := cfg.hooks.(hooks.ProducerHooks)
	if !ok {
		if cfg.hooks != nil {
			panic("invalid hooks")
		}
		phk = hooks.HookProducer()
	}

	var kp *kafka.Producer
	var err error
	if kp, err = phk.Create(cfg.config.configMap()); err != nil {
		return nil, err
	}

	return &producer{
		hooks:          phk,
		config:         cfg.copy(),
		producer:       kp,
		middleware:     cfg.middleware,
		DeliveryEvents: phk.GetEventChannel(kp),
	}, nil
}

func (p *producer) Close() {
	p.hooks.Close(p.producer)
}

func (p *producer) Flush(timeoutMs int) int {
	return p.hooks.Flush(p.producer, timeoutMs)
}

func (p *producer) FlushAll() {
	for {
		r := p.Flush(100)
		if r == 0 {
			return
		}
	}
}

// HandleEvents starts a goroutine that processes events arriving on the producer
// Event channel.  Events are dispatched to the appropriate method of the
// supplied event handler interface.
func (p *producer) HandleEvents(ctx context.Context, handler ProducerEventHandler) {
	go func() {
		defer p.Close()

		close := false

		for e := range p.hooks.GetEventChannel(p.producer) {
			switch ev := e.(type) {
			case *kafka.Message:
				// The message delivery report, indicating success or
				// permanent failure after retries have been exhausted.
				if err := ev.TopicPartition.Error; err != nil {
					handler.OnMessageError(ev, ev.TopicPartition.Error)
				} else {
					handler.OnMessageDelivered(ev)
				}
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				handler.OnProducerError(ev, &close)
			default:
				handler.OnUnexpectedEvent(ev, &close)
			}
			// If a ProducerError or UnexpectedEvent handler signals that the
			// producer should close then return from the goroutine, which
			// which also close the producer
			if close {
				return
			}
		}
	}()
}

// MustProduce produces a message and waits for a delivery event.  The produced
// message is returned if successful, otherwise an error is returned.
func (p *producer) MustProduce(msg *kafka.Message) (*kafka.Message, error) {

	if msg.TopicPartition.Topic == nil || *msg.TopicPartition.Topic == "" {
		return nil, &ErrNoTopicId{message: "message has no topic id"}
	}

	dc := make(chan kafka.Event)
	defer close(dc)

	if err := p.hooks.Produce(p.producer, msg, dc); err != nil {
		return nil, err
	}

	event := <-dc

	return CheckEvent(event)
}

// Produce produces a message.  Delivery events are received over the producer.EventChannel
func (p *producer) Produce(msg *kafka.Message) error {
	return p.hooks.Produce(p.producer, msg, nil)
}

// CheckEvent examines the specified kafka.Event.  If was a message related
// event then the message is returned; if the message had an error, this also
// is returned.
//
// If the event was a kafka.Error (producer client error) then a nil message
// is returned together with the error.
//
// In all other cases the event is returned as an ErrUnexpectedDeliveryEvent
// (with no message).
func CheckEvent(event kafka.Event) (*kafka.Message, error) {
	switch event := event.(type) {
	case *kafka.Message:
		if event.TopicPartition.Error != nil {
			return event, event.TopicPartition.Error
		}
		return event, nil

	case kafka.Error:
		return nil, event
	}

	return nil, ErrUnexpectedDeliveryEvent{event: event}
}

// MustProduce creates a temporary producer from a specified Config to produce
// a specified message.  The temporary producer is closed immediately that the
// message is delivered or a delivery error is returned.
func MustProduce(cfg *config, msg *kafka.Message) (*kafka.Message, error) {

	if msg.TopicPartition.Topic == nil || *msg.TopicPartition.Topic == "" {
		return nil, &ErrNoTopicId{message: "message has no topic id"}
	}

	prod, err := NewProducer(cfg)
	if err != nil {
		return nil, err
	}
	defer prod.Close()

	return prod.MustProduce(msg)
}
