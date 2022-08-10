package mock

import (
	"errors"
	"fmt"
	"kafka/api"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Message(t string, v interface{}) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &t,
		},
		Value: v.([]byte),
	}
}

func MessageWithString(t string, v string) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &t,
		},
		Value: []byte(v),
	}
}

func Messages(msgs []interface{}) api.MessageReader {
	return &messages{
		idx:  -1,
		msgs: msgs,
	}
}

type messages struct {
	idx  int
	msgs []interface{}
}

func (r *messages) ReadMessage(c *kafka.Consumer, t time.Duration) (*kafka.Message, error) {
	r.idx++

	if r.idx == len(r.msgs) {
		return nil, errors.New("no more messages")
	}

	next := r.msgs[r.idx]
	switch v := next.(type) {
	case *kafka.Message:
		return v, nil
	case error:
		return nil, v
	default:
		return nil, fmt.Errorf("unexpected item of type %T in mock message list", v)
	}
}

type noMessages struct{}

func (*noMessages) ReadMessage(c *kafka.Consumer, t time.Duration) (*kafka.Message, error) {
	return nil, errors.New("no messages")
}
