package goxstreams

import (
	"context"
)

// ProducerConverter is an interface for converting business model to hash.
type ProducerConverter[E any] interface {
	From(event E) map[string]interface{}
}

// Producer is a wrapper to easily produce messages to redis stream.
type Producer[E any] struct {
	client    streamClient
	converter ProducerConverter[E]
}

// NewProducer is a constructor Producer struct.
func NewProducer[E any](client RedisClient, converter ProducerConverter[E]) Producer[E] {
	streamClient := newClient(client, clientParams{})

	return Producer[E]{client: streamClient, converter: converter}
}

// Produce method for push message to redis stream.
func (p Producer[E]) Produce(ctx context.Context, event E, stream string) error {
	return p.client.add(ctx, stream, p.converter.From(event))
}
