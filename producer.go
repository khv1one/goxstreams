package goxstreams

import (
	"context"
)

// Producer is a wrapper to easily produce messages to redis stream.
type Producer[E any] struct {
	client    streamClient
}

// NewProducer is a constructor Producer struct.
func NewProducer[E any](client RedisClient) Producer[E] {
	streamClient := newClient(client, clientParams{})

	return Producer[E]{client: streamClient}
}

// Produce method for push message to redis stream.
func (p Producer[E]) Produce(ctx context.Context, event E, stream string) error {
	eventData, err := convertFrom(event)
	if err != nil {
		return err
	}

	return p.client.add(ctx, stream, eventData)
}
