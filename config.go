package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

type ConfigMap map[string]interface{}
type MessageMiddleware func([]byte) ([]byte, error)
type TopicHandler func(context.Context, []byte) error
type TopicHandlerMap map[string]TopicHandler

type Config struct {
	mockApi            bool
	ctx                context.Context
	cm                 ConfigMap
	maxProducerRetries int
	middleware         MessageMiddleware
	handlers           TopicHandlerMap
	synchronous        bool
}

func NewConfig() *Config {
	return &Config{
		cm:       ConfigMap{},
		handlers: TopicHandlerMap{},
	}
}

func (c *Config) ConfigMap() *kafka.ConfigMap {
	cm := kafka.ConfigMap{}
	for k, v := range c.cm {
		cm[k] = v
	}
	return &cm
}

func (c *Config) Copy() *Config {
	return &Config{
		mockApi:    c.mockApi,
		middleware: c.middleware,
		cm:         c.Map(),
		handlers:   c.TopicHandlers(),
	}
}

func (c *Config) Map() ConfigMap {
	cm := ConfigMap{}
	for k, v := range c.cm {
		cm[k] = v
	}
	return cm
}

func (c *Config) AutoCommitEnabled() bool {
	enabled, ok := c.cm[key[enableAutoCommit]]
	return !ok || enabled.(bool)
}

func (c *Config) Merge(cm *ConfigMap) *Config {
	for k, v := range *cm {
		c.cm[k] = v
	}
	return c
}

func (c *Config) TopicHandlers() TopicHandlerMap {
	thm := TopicHandlerMap{}
	for k, v := range c.handlers {
		thm[k] = v
	}
	return thm
}

func (c *Config) TopicIds() []string {
	ta := []string{}
	for k := range c.handlers {
		ta = append(ta, k)
	}
	return ta
}

func (c *Config) WithMockApi() *Config {
	r := c.Copy()
	r.mockApi = true
	return r
}

func (c *Config) With(k string, v interface{}) *Config {
	r := c.Copy()
	r.cm[k] = v
	return r
}

func (c *Config) WithContext(ctx context.Context) *Config {
	r := c.Copy()
	r.ctx = ctx
	return r
}

func (c *Config) WithAutoCommit(v bool) *Config {
	r := c.Copy()
	r.cm[key[enableAutoCommit]] = v
	return r
}

func (c *Config) WithBootstrapServers(s string) *Config {
	r := c.Copy()
	r.cm[key[bootstrapServers]] = s
	return r
}

func (c *Config) WithMiddleware(m MessageMiddleware) *Config {
	r := c.Copy()
	r.middleware = m
	return r
}

func (c *Config) WithGroupId(s string) *Config {
	r := c.Copy()
	r.cm[key[groupId]] = s
	return r
}

func (c *Config) WithIdempotence(v bool) *Config {
	r := c.Copy()
	r.cm[key[enableIdempotence]] = v
	return r
}

func (c *Config) WithMaxProducerRetries(i int) *Config {
	r := c.Copy()

	if i < 0 {
		i = 0
		log.Warning("Attempted to Config.WithMaxProducerRetries < 0 (0 was applied, no retries)")
	}

	if i > 5 {
		i = 5
		log.Warning("Attempted to Config.WithMaxProducerRetries > 5 (5 was applied)")
	}

	r.maxProducerRetries = i
	return r
}

func (c *Config) WithSynchronous(v bool) *Config {
	r := c.Copy()
	r.synchronous = v
	return r
}

func (c *Config) WithTopicHandler(t string, fn TopicHandler) *Config {
	r := c.Copy()
	r.handlers[t] = fn
	return r
}

const (
	bootstrapServers = iota
	enableAutoCommit
	enableIdempotence
	groupId
)

var key = map[int]string{
	bootstrapServers:  "bootstrap.servers",
	enableAutoCommit:  "enable.auto.commit",
	enableIdempotence: "enable.idempotence",
	groupId:           "group.id",
}
