package mock

import (
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

func StringMessage(t string, v string) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &t,
		},
		Value: []byte(v),
	}
}
