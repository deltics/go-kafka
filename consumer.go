package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"

	"kafka/api"
	"kafka/mock"
)

type Consumer struct {
	api        api.ConsumerHooks
	config     *Config
	consumer   *kafka.Consumer
	handlers   TopicHandlerMap
	middleware MessageMiddleware
}

func NewConsumer(cfg *Config) (*Consumer, error) {
	api := api.Consumer
	if cfg.mockApi {
		api = mock.Consumer
	}

	var kc *kafka.Consumer
	var err error
	if kc, err = api.Create(cfg.ConfigMap()); err != nil {
		log.WithError(err).
			Error("Failed to create kafka consumer")
		return nil, err
	}

	return &Consumer{
		api:        api,
		config:     cfg.Copy(),
		consumer:   kc,
		middleware: cfg.middleware,
		handlers:   cfg.handlers,
	}, nil
}

func (c *Consumer) Close() {
	c.api.Close(c.consumer)
}

func (c *Consumer) Run() {
	defer c.Close()

	ctx := c.config.ctx
	if ctx == nil {
		log.Warning("no context")
		ctx = context.TODO()
	}
	autoCommit := c.config.AutoCommitEnabled()

	if err := c.api.Subscribe(c.consumer, c.config.TopicIds(), nil); err != nil {
		log.WithError(err).
			Error("error subscribing to topics")
		return
	}

	for {
		msg, err := c.api.MessageReader.ReadMessage(c.consumer, -1)
		if err != nil {
			// TODO: Check msg for topic/partition info to include in error log
			log.Error(err)
			break
		}

		if msg == nil {
			continue
		}

		bytes := msg.Value
		if c.middleware != nil {
			bytes, err = c.middleware(bytes)
			if err != nil {
				log.Error(err)
				continue
			}
		}

		handler, ok := c.handlers[*msg.TopicPartition.Topic]
		if !ok {
			log.Warningf("no handler for topic %v", *msg.TopicPartition.Topic)
			continue
		}

		err = handler(ctx, bytes)
		if err == nil && !autoCommit {
			_, err = c.api.CommitOffset(c.consumer, []kafka.TopicPartition{msg.TopicPartition})
			if err != nil {
				log.WithError(err).
					Error("failed to commit offset")
			}
		}
	}
}
