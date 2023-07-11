package producer

import (
	"context"
)

type Client[E any] interface {
	Add(ctx context.Context, stream string, event E, from func(event E) map[string]interface{}) error
}

type Converter[E any] interface {
	From(event E) map[string]interface{}
}

type Producer[E any] struct {
	client    Client[E]
	converter Converter[E]
}

func NewProducer[E any](client Client[E], converter Converter[E]) Producer[E] {
	return Producer[E]{client: client, converter: converter}
}

func (p Producer[E]) Produce(ctx context.Context, event E, stream string) error {
	return p.client.Add(ctx, stream, event, p.converter.From)
}
