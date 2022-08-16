package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ErrUnexpectedDeliveryEvent struct {
	event kafka.Event
}

func (e ErrUnexpectedDeliveryEvent) Error() string {
	return fmt.Sprintf("unexpected delivery event (%T): %[1]s", e.event)
}

type ErrNoTopicId struct {
	message string
}

func (e ErrNoTopicId) Error() string {
	return "message had no topic id"
}
