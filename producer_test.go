package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/deltics/go-kafka/hooks"
	"github.com/deltics/go-kafka/mock"
)

func TestThatCheckEventReturnsMessageForASuccessfulDelivery(t *testing.T) {
	// ARRANGE
	topic := "topic"
	wantedError := error(nil)
	wantedMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 1,
			Offset:    1,
		},
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	// ACT
	gotMessage, gotError := CheckEvent(wantedMessage)

	// ASSERT
	if gotError != wantedError {
		t.Errorf("wanted error %v, got %v", wantedError, gotError)
	}
	if wantedMessage != gotMessage {
		t.Errorf("wanted message %v, got %v", wantedMessage, gotMessage)
	}
}

func TestThatCheckEventReturnsMessageAndErrorForMessageError(t *testing.T) {
	// ARRANGE
	topic := "topic"
	wantedError := errors.New("error")
	wantedMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 1,
			Offset:    1,
			Error:     wantedError,
		},
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	// ACT
	gotMessage, gotError := CheckEvent(wantedMessage)

	// ASSERT
	if gotError != wantedError {
		t.Errorf("wanted error %v, got %v", wantedError, gotError)
	}
	if wantedMessage != gotMessage {
		t.Errorf("wanted message %v, got %v", wantedMessage, gotMessage)
	}
}

func TestThatCheckEventReturnsErrorForKafkaError(t *testing.T) {
	// ARRANGE
	wanted := kafka.NewError(kafka.ErrBrokerNotAvailable, "error", true)

	// ACT
	msg, got := CheckEvent(wanted)

	// ASSERT
	if wanted != got {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
	if msg != nil {
		t.Errorf("unexpected message returned: %v", msg)
	}
}

func TestThatNewProducerFromConfigWithNoHooksIsHookedCorrectly(t *testing.T) {
	// ARRANGE
	cfg := NewConfig().WithNoClient()

	// ACT
	p, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := reflect.TypeOf(hooks.HookProducer())
	got := reflect.TypeOf(p.hooks)
	if got != wanted {
		t.Errorf("wanted %T, got %T", wanted, got)
	}
}

func TestThatNewProducerPanicsIfConfigHasConsumerHooks(t *testing.T) {
	// ARRANGE
	cfg := NewConfig().WithHooks(mock.ConsumerHooks())
	defer func() {
		if r := recover(); r == nil {
			t.Error("did not panic")
		}
	}()

	// ACT
	_, err := NewProducer(cfg)

	// ASSERT (see above)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestThatCloseCallsCloseOnTheProducer(t *testing.T) {

	closeCalled := false

	// ARRANGE
	hk := mock.ProducerHooks()
	hk.Funcs().Close = func(p *kafka.Producer) { closeCalled = true }

	cfg := NewConfig().WithHooks(hk)

	// ACT
	p, err := NewProducer(cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	p.Close()

	// ASSERT
	if closeCalled != true {
		t.Error("Close() was not called")
	}
}

func TestThatFlushCallsFlushOnTheProducer(t *testing.T) {

	flushCalled := false

	// ARRANGE
	hk := mock.ProducerHooks()
	hk.Funcs().Flush = func(p *kafka.Producer, timeoutMs int) int { flushCalled = true; return 0 }

	cfg := NewConfig().WithHooks(hk)

	// ACT
	p, err := NewProducer(cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	p.Flush(100)

	// ASSERT
	if flushCalled != true {
		t.Error("Flush() was not called")
	}
}

func TestThatFlushAllCallsFlushOnTheProducerUntilZeroIsReturned(t *testing.T) {

	flushCalls := 0
	flushesRemaining := 10

	// ARRANGE
	hk := mock.ProducerHooks()
	hk.Funcs().Flush = func(p *kafka.Producer, timeoutMs int) int {
		flushCalls++
		flushesRemaining -= 1
		return flushesRemaining
	}

	cfg := NewConfig().WithHooks(hk)

	// ACT
	p, err := NewProducer(cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	p.FlushAll()

	// ASSERT
	wanted := 10
	got := flushCalls
	if wanted != got {
		t.Errorf("wanted %d Flush() calls, got %d", wanted, got)
	}

	wanted = 0
	got = flushesRemaining
	if wanted != got {
		t.Errorf("wanted %d flushes remaining, got %d", wanted, got)
	}
}

func TestThatMustProduceReturnsSuccessfullyDeliveredMessage(t *testing.T) {
	// ARRANGE
	topic := "test"
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          []byte("test value"),
	}
	wanted := msg
	wanted.TopicPartition.Partition = 1
	wanted.TopicPartition.Offset = 1

	hk := mock.ProducerHooks()
	hk.Funcs().Produce = func(p *kafka.Producer, m *kafka.Message, c chan kafka.Event) error {
		go func() {
			m.TopicPartition.Partition = 1
			m.TopicPartition.Offset = 1
			c <- m
		}()
		return nil
	}

	cfg := NewConfig().WithHooks(hk)

	// ACT
	got, err := MustProduce(context.Background(), cfg, &msg)

	// ASSERT
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got.TopicPartition != wanted.TopicPartition {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}
func TestThatMustProduceReturnsMessageDeliveryErrorWithFailedMessage(t *testing.T) {
	// ARRANGE
	topic := "test"
	deliveryErr := errors.New("delivery failed")
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          []byte("test value"),
	}
	wanted := msg
	wanted.TopicPartition.Error = deliveryErr

	hk := mock.ProducerHooks()
	hk.Funcs().Produce = func(p *kafka.Producer, m *kafka.Message, c chan kafka.Event) error {
		go func() {
			m.TopicPartition.Error = deliveryErr
			c <- m
		}()
		return nil
	}

	cfg := NewConfig().WithHooks(hk)

	// ACT
	got, err := MustProduce(context.Background(), cfg, &msg)

	// ASSERT
	if err == nil {
		t.Errorf("expected error not returned, wanted %q", err)
	}

	if got.TopicPartition != wanted.TopicPartition {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}

func TestThatMustProduceReturnsProducerErrorWithNoMessage(t *testing.T) {
	// ARRANGE
	topic := "test"
	deliveryError := kafka.NewError(101, "retrying", false)
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          []byte("test value"),
	}

	hk := mock.ProducerHooks()
	hk.Funcs().Produce = func(p *kafka.Producer, m *kafka.Message, c chan kafka.Event) error {
		go func() {
			c <- deliveryError
		}()
		return nil
	}

	cfg := NewConfig().WithHooks(hk)

	// ACT
	deliveredMsg, err := MustProduce(context.Background(), cfg, &msg)

	// ASSERT
	if deliveredMsg != nil {
		t.Errorf("unexpected message returned: %v", deliveredMsg)
	}

	if got, ok := err.(kafka.Error); !ok {
		wanted := deliveryError
		t.Errorf("wanted %T, got %T", wanted, got)
	}

	wanted := deliveryError.Error()
	got := err.Error()
	if wanted != got {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}

type fakeevent struct{}

func (*fakeevent) String() string { return "fake event" }

func TestThatMustProduceReturnsUnexpectedEventsWithNoMessage(t *testing.T) {
	// ARRANGE
	topic := "test"
	var deliveryError kafka.Event = &fakeevent{}
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          []byte("test value"),
	}

	hk := mock.ProducerHooks()
	hk.Funcs().Produce = func(p *kafka.Producer, m *kafka.Message, c chan kafka.Event) error {
		go func() {
			c <- deliveryError
		}()
		return nil
	}

	cfg := NewConfig().WithHooks(hk)

	// ACT
	deliveredMsg, err := MustProduce(context.Background(), cfg, &msg)

	// ASSERT
	if deliveredMsg != nil {
		t.Errorf("unexpected message returned: %v", deliveredMsg)
	}

	if got, ok := err.(ErrUnexpectedDeliveryEvent); !ok {
		wanted := ErrUnexpectedDeliveryEvent{}
		t.Errorf("wanted %T, got %T", wanted, got)
	}

	wanted := ErrUnexpectedDeliveryEvent{event: deliveryError}.Error()
	got := err.(ErrUnexpectedDeliveryEvent).Error()
	if got != wanted {
		t.Errorf("wanted %q, got %q", wanted, got)
	}
}
