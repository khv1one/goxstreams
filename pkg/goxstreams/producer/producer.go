package producer

import (
	"context"
)

type Client[E any] interface {
	Add(ctx context.Context, stream string, event E) error
}

type Producer[E any] struct {
	client Client[E]
}

func NewProducer[E any](client Client[E]) Producer[E] {
	return Producer[E]{client: client}
}

func (p Producer[E]) Produce(ctx context.Context, event E, stream string) error {
	return p.client.Add(ctx, stream, event)
}
