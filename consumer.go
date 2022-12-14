package kafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/deltics/go-kafka/hooks"
)

type Consumer struct {
	hooks      hooks.ConsumerHooks
	config     *config
	consumer   *kafka.Consumer
	handlers   messageHandlerMap
	middleware MessageMiddleware
}

func NewConsumer(cfg *config) (*Consumer, error) {
	// Assume standard consumer hooks by default
	hk := hooks.HookConsumer()

	// Apply any alternative hooks from the config (if valid)
	if cfg.hooks != nil {
		var ok bool
		if hk, ok = cfg.hooks.(hooks.ConsumerHooks); !ok {
			panic(fmt.Sprintf("invalid hooks (%T): not valid for a consumer", cfg.hooks))
		}
	}

	// Create the consumer
	var kc *kafka.Consumer
	var err error
	if kc, err = hk.Create(cfg.config.configMap()); err != nil {
		return nil, err
	}

	return &Consumer{
		hooks:      hk,
		config:     cfg.copy(),
		consumer:   kc,
		middleware: cfg.middleware,
		handlers:   cfg.messageHandlers.copy(),
	}, nil
}

func (c *Consumer) Close() {
	c.hooks.Close(c.consumer)
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.Close()

	autoCommit := c.config.autoCommit()

	if err := c.hooks.Subscribe(c.consumer, c.config.messageHandlers.topicIds(), nil); err != nil {
		return err
	}

	for {
		msg, err := c.hooks.ReadMessage(c.consumer, -1)
		if err != nil {
			// TODO: Check msg for topic/partition info to include in error log
			return err
		}

		if msg == nil {
			continue
		}

		// Ensure we have a handler (since we subscribe to topics with handlers, this
		// shouldn't be necessary so if it does happen, it's a panic!)
		handler, ok := c.handlers[*msg.TopicPartition.Topic]
		if !ok {
			panic(fmt.Sprintf("no handler for topic %v", *msg.TopicPartition.Topic))
		}

		// TODO: If middleware returns an error shouldn't we stop consuming? (or at least
		//       give the app/service the option, via a callback notification)
		if c.middleware != nil {
			msg, err = c.middleware(msg)
			if err != nil {
				// TODO: callback notifications
				continue
			}
		}

		err = handler(ctx, msg)
		if err == nil && !autoCommit {
			_, err = c.hooks.CommitOffset(c.consumer, []kafka.TopicPartition{msg.TopicPartition})
			if err != nil {
				return err
			}
		}
	}
}
