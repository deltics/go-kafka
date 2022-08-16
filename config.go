package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	_hooks "github.com/deltics/go-kafka/hooks"
)

type MessageMiddleware func(*kafka.Message) (*kafka.Message, error)
type MessageHandler func(context.Context, *kafka.Message) error

type config struct {
	hooks      interface{}
	config     configMap
	middleware MessageMiddleware
	// Consumer-only members
	messageHandlers messageHandlerMap // map of topic-name:handler
}

func NewConfig() *config {
	return &config{
		config:          configMap{},
		messageHandlers: messageHandlerMap{},
	}
}

func (c *config) copy() *config {
	return &config{
		hooks:           c.hooks,
		middleware:      c.middleware,
		config:          c.config.copy(),
		messageHandlers: c.messageHandlers.copy(),
	}
}

func (c *config) autoCommit() bool {
	enabled, ok := c.config[key[enableAutoCommit]]
	return !ok || enabled.(bool)
}

func (c *config) With(key string, value interface{}) *config {
	r := c.copy()
	r.config[key] = value
	return r
}

func (c *config) WithAutoCommit(v bool) *config {
	r := c.copy()
	r.config[key[enableAutoCommit]] = v
	return r
}

func (c *config) WithBatchSize(size int) *config {
	r := c.copy()
	r.config[key[batchSize]] = size
	return r
}

func (c *config) WithBootstrapServers(servers interface{}) *config {
	r := c.copy()

	switch servers := servers.(type) {
	case []string:
		r.config[key[bootstrapServers]] = strings.Join(servers, ",")
	case string:
		r.config[key[bootstrapServers]] = servers
	default:
		panic(fmt.Sprintf("servers is %T: must be string or []string", servers))
	}

	return r
}

func (c *config) WithHooks(hooks interface{}) *config {
	_, consumerHooks := hooks.(_hooks.ConsumerHooks)
	_, producerHooks := hooks.(_hooks.ProducerHooks)

	if !consumerHooks && !producerHooks {
		panic("invalid hooks; must implement ConsumerHooks or ProducerHooks")
	}

	r := c.copy()
	r.hooks = hooks
	return r
}

func (c *config) WithMiddleware(middleware MessageMiddleware) *config {
	r := c.copy()
	r.middleware = middleware
	return r
}

// WithNoClient returns a Config configured to prevent Consumer or Provider
// initialisation from connecting a client to any broker.  This is
// intended for use in TESTS only.
//
// Attempting to use a Consumer or Producer configured with this setting is
// unsupported and is likely to result in errors, panics or other
// unpredictable behaviour.
//
// This has limited use cases but is necessary to test certain aspects of the
// Consumer and Producer client hooking mechanism.
//
// For comprehensive mocking, faking and stubbing use WithHooks().
func (c *config) WithNoClient() *config {
	return c.WithBootstrapServers("test://noclient")
}

func (c *config) WithGroupId(s string) *config {
	r := c.copy()
	r.config[key[groupId]] = s
	return r
}

func (c *config) WithIdempotence(v bool) *config {
	r := c.copy()
	r.config[key[enableIdempotence]] = v
	return r
}

func (c *config) WithMessageHandler(t string, fn MessageHandler) *config {
	r := c.copy()
	r.messageHandlers[t] = fn
	return r
}
