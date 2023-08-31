package goxstreams

import (
	"context"
)

type Converter[E any] interface {
	From(event E) map[string]interface{}
}

type Producer[E any] struct {
	client    streamClient
	converter Converter[E]
}

func NewProducer[E any](client RedisClient, converter Converter[E]) Producer[E] {
	streamClient := newClient(client, clientParams{})

	return Producer[E]{client: streamClient, converter: converter}
}

func (p Producer[E]) Produce(ctx context.Context, event E, stream string) error {
	return p.client.add(ctx, stream, p.converter.From(event))
}
