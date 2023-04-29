package consumer

import (
	"context"
)

type RawEvent interface {
	GetId() string
	GetBody() map[string]interface{}
}

type Client[E any] interface {
	ReadGroup(ctx context.Context) ([]E, error)
	Pending()
	Claim()
	Ack()
}

type Converter[E any] interface {
	To(id string, event map[string]interface{}) (E, error)
}

type Consumer[E any] struct {
	client    Client[E]
	converter Converter[E]
}

func NewConsumer[E any](client Client[E], converter Converter[E]) Consumer[E] {
	return Consumer[E]{client: client, converter: converter}
}

func (c Consumer[E]) ReadGroup(ctx context.Context) ([]E, error) {
	return c.client.ReadGroup(ctx)
}
