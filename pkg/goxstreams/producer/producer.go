package producer

import (
	"context"

	sc "github.com/khv1one/goxstreams/pkg/goxstreams/client"
	"github.com/redis/go-redis/v9"
)

// RedisClient required to use cluster client
type RedisClient interface {
	redis.Cmdable
}

type Converter[E any] interface {
	From(event E) map[string]interface{}
}

type Producer[E any] struct {
	client    sc.StreamClient
	converter Converter[E]
}

func NewProducer[E any](client RedisClient, converter Converter[E]) Producer[E] {
	streamClient := sc.NewClient(client, sc.Params{})

	return Producer[E]{client: streamClient, converter: converter}
}

func (p Producer[E]) Produce(ctx context.Context, event E, stream string) error {
	return p.client.Add(ctx, stream, p.converter.From(event))
}
