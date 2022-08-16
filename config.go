package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConfigMap map[string]interface{}
type MessageMiddleware func(*kafka.Message) (*kafka.Message, error)
type TopicHandler func(context.Context, *kafka.Message) error
type TopicHandlerMap map[string]TopicHandler

type config struct {
	hooks      interface{}
	config     ConfigMap
	middleware MessageMiddleware
	// Consumer-only members
	ctx      context.Context // passed in topic handler calls
	handlers TopicHandlerMap // map of topic-name:handler
}

func NewConfig() *config {
	return &config{
		config:   ConfigMap{},
		handlers: TopicHandlerMap{},
	}
}

func (c *config) copy() *config {
	return &config{
		hooks:      c.hooks,
		middleware: c.middleware,
		config:     c.config.copy(),
		handlers:   c.handlers.copy(),
	}
}

func (cm ConfigMap) copy() ConfigMap {
	copy := ConfigMap{}
	for k, v := range cm {
		copy[k] = v
	}
	return copy
}

func (cm ConfigMap) configMap() *kafka.ConfigMap {
	kcm := kafka.ConfigMap{}
	for k, v := range cm {
		kcm[k] = v
	}
	return &kcm
}

func (thm TopicHandlerMap) copy() TopicHandlerMap {
	copy := TopicHandlerMap{}
	for k, v := range thm {
		copy[k] = v
	}
	return copy
}

func (thm TopicHandlerMap) topicIds() []string {
	ids := make([]string, 0, len(thm))
	for k := range thm {
		ids = append(ids, k)
	}
	return ids
}

func (c *config) autoCommit() bool {
	enabled, ok := c.config[key[enableAutoCommit]]
	return !ok || enabled.(bool)
}

func (c *config) With(k string, v interface{}) *config {
	r := c.copy()
	r.config[k] = v
	return r
}

func (c *config) WithBatchSize(size int) *config {
	r := c.copy()
	r.config[key[batchSize]] = size
	return c
}

func (c *config) WithHooks(hooks interface{}) *config {
	r := c.copy()
	r.hooks = hooks
	if r.config[key[bootstrapServers]] == "" {
		r.config[key[bootstrapServers]] = "mock"
	}
	return r
}

func (c *config) WithContext(ctx context.Context) *config {
	r := c.copy()
	r.ctx = ctx
	return r
}

func (c *config) WithAutoCommit(v bool) *config {
	r := c.copy()
	r.config[key[enableAutoCommit]] = v
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

func (c *config) WithMiddleware(m MessageMiddleware) *config {
	r := c.copy()
	r.middleware = m
	return r
}

// WithNoClient is for use in tests only.  It returns a Config with `bootstrap.servers`
// set to `test://noclient` which results in the Consumer or Provider initialising
// a dummy client rather than attempting to connect a real client to any broker.
//
// Attempting to use a Consumer or Producer configured with this setting is
// unsupported and is likely to result in panics or unpredictable behaviour.
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

func (c *config) WithTopicHandler(t string, fn TopicHandler) *config {
	r := c.copy()
	r.handlers[t] = fn
	return r
}
