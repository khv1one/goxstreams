package producer

import (
	"context"

	"github.com/khv1one/goxstreams/pkg/goxstreams/client"
)

type Converter[E any] interface {
	From(event E) map[string]interface{} // TODO: добавить возможность ошибки
}

type Producer[E any] struct {
	client    client.StreamClient
	converter Converter[E]
}

func NewProducer[E any](client client.StreamClient, converter Converter[E]) Producer[E] {
	return Producer[E]{client: client, converter: converter}
}

func (p Producer[E]) Produce(ctx context.Context, event E, stream string) error {
	return p.client.Add(ctx, stream, p.converter.From(event))
}
