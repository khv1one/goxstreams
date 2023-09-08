package goxstreams

import (
	"context"
)

// Producer is a wrapper to easily produce messages to redis stream.
type Producer[E any] struct {
	client      streamClient
	convertFrom func(event *E) (map[string]interface{}, error)
}

// NewProducer is a constructor Producer struct.
func NewProducer[E any](client RedisClient) Producer[E] {
	streamClient := newClient(client, clientParams{})

	return Producer[E]{client: streamClient, convertFrom: convertFrom[*E]}
}

// NewProducerWithConverter is a constructor Producer struct with custom converter.
//
// Since Redis Streams messages are limited to a flat structure, we have 2 options available:
//   - flat Example: ("foo_key", "foo_val", "bar_key", "bar_val");
//   - nested json or proto into one key ("key", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
//   - or combination ("foo_key", "foo_val", "foobar", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
func NewProducerWithConverter[E any](client RedisClient, convertFrom func(event *E) (map[string]interface{}, error)) Producer[E] {
	producer := NewProducer[E](client)
	producer.convertFrom = convertFrom
	return producer
}

// Produce method for push message to redis stream.
//
// With default converter, redis message will be like:
//   - "xadd" "mystream" "*" "body" "{\"Message\":\"message\",\"Name\":\"name\",\"Foo\":712,\"Bar\":947}"
func (p Producer[E]) Produce(ctx context.Context, event E, stream string) error {
	eventData, err := p.convertFrom(&event)
	if err != nil {
		return err
	}

	return p.client.add(ctx, stream, eventData)
}
