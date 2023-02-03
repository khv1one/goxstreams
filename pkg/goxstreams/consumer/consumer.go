package consumer

import (
	"context"
)

type Client[E any] interface {
	GroupRead(ctx context.Context) (map[string][]E, error)
	Pending()
	Claim()
	Ack()
}

type Consumer[E any] struct {
	client Client[E]
}

func NewConsumer[E any](client Client[E]) Consumer[E] {
	return Consumer[E]{client: client}
}
