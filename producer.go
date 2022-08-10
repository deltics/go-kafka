package kafka

import (
	"context"
	"kafka/hooks"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

type ETimeOut struct{}

func (e ETimeOut) Error() string { return "timed out" }

type producer struct {
	hooks      hooks.ProducerHooks
	config     *Config
	producer   *kafka.Producer
	middleware MessageMiddleware
}

func NewProducer(cfg *Config) (*producer, error) {
	phk, ok := cfg.hooks.(hooks.ProducerHooks)
	if !ok {
		if phk != nil {
			panic("invalid provider")
		}
		phk = hooks.HookProducer()
	}

	var kp *kafka.Producer
	var err error
	if kp, err = phk.Create(cfg.ConfigMap()); err != nil {
		log.WithError(err).
			Error("Failed to create kafka producer")
		return nil, err
	}

	return &producer{
		hooks:      phk,
		config:     cfg.Copy(),
		producer:   kp,
		middleware: cfg.middleware,
	}, nil
}

func (p *producer) Close() {
	p.hooks.Close(p.producer)
}

func (p *producer) Run() {
	defer p.Close()

	ctx := p.config.ctx
	if ctx == nil {
		log.Warning("no context, using Background()")
		ctx = context.Background()
	}

	go func() {
		for e := range p.hooks.Events(p.producer) {
			switch ev := e.(type) {
			case *kafka.Message:
				// The message delivery report, indicating success or
				// permanent failure after retries have been exhausted.
				// Application level retries won't help since the client
				// is already configured to do that.
				m := ev
				if m.TopicPartition.Error != nil {
					log.WithError(m.TopicPartition.Error).
						Error("Delivery failed")
				} else {
					log.Info("Delivered message")
				}
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				log.Warning(ev)
			default:
				log.Info("Ignored event")
			}
		}
	}()
}

// TODO: Error types
func (p *producer) Produce(msg *kafka.Message, ch chan kafka.Event) error {
	// Initialise the max retry count, constrained to the range 0 (no retry)..5
	//
	// With an initial 1/2 second delay and exponential back-off,
	// these constraints result in a maximum wait time of ~13secs and a
	// total failure time (when all retries fail and the producer gives up)
	// of ~25secs

	r := p.config.maxProducerRetries
	d := 500 * time.Millisecond

	for r >= 0 {
		err := p.hooks.Produce(p.producer, msg, ch)

		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull && r > 0 {
				// Producer queue is full, wait before trying again
				// and decrement the retry counter
				time.Sleep(d)
				d *= 2
				r--
				continue
			}
			log.WithError(err).
				Error("failed to produce message")
		}

		return err
	}

	err := ETimeOut{}
	log.WithError(err).
		Error("failed to produce message")
	return err
}
