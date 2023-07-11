package consumer

import (
	"context"
	"fmt"
)

type RedisEvent interface {
	GetRedisID() string
}

type Client[E RedisEvent] interface {
	ReadGroup(ctx context.Context, to func(string, map[string]interface{}) (E, error)) ([]E, error)
	Pending()
	Claim()
	Ack(ctx context.Context, id string) error
}

type Converter[E RedisEvent] interface {
	To(id string, event map[string]interface{}) (E, error)
}

type Consumer[E RedisEvent] struct {
	client       Client[E]
	converter    Converter[E]
	eventProcess func(E) error
}

func NewConsumer[E RedisEvent](client Client[E], converter Converter[E], eventProcess func(E) error) Consumer[E] {
	return Consumer[E]{client: client, converter: converter, eventProcess: eventProcess}
}

func (c Consumer[E]) Run(ctx context.Context) {
	go c.run(ctx)
}

func (c Consumer[E]) run(ctx context.Context) {
	for {
		events, err := c.client.ReadGroup(ctx, c.converter.To)
		if err != nil {
			fmt.Printf("read error %w\n", err)
			continue
		}

		for _, event := range events {
			c.processEvent(ctx, event)
		}
	}
}

func (c Consumer[E]) processEvent(ctx context.Context, event E) {
	err := c.eventProcess(event)
	if err != nil {
		return //TODO process error
	}

	c.client.Ack(ctx, event.GetRedisID())
}
