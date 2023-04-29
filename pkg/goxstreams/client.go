package goxstreams

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

type Converter[E any] interface {
	To(id string, event map[string]interface{}) (E, error)
	From(event E) map[string]interface{}
}

type StreamClient[E any] struct {
	client        *redis.Client
	groupReadArgs *redis.XReadGroupArgs
	converter     Converter[E]
}

type Params struct {
	Stream   string
	Group    string
	Consumer string
	Batch    int64
}

func NewClient[E any](client *redis.Client, params Params, converter Converter[E]) StreamClient[E] {
	groupReadArgs := &redis.XReadGroupArgs{
		Streams:  []string{params.Stream, ">"},
		Group:    params.Group,
		Consumer: params.Consumer,
		Count:    params.Batch,
		NoAck:    true,
	}

	streamClient := StreamClient[E]{
		client:        client,
		groupReadArgs: groupReadArgs,
		converter:     converter,
	}

	return streamClient
}

func (c StreamClient[E]) Add(ctx context.Context, stream string, event E) error {
	_, err := c.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: c.converter.From(event)}).Result()

	return err
}

func (c StreamClient[E]) ReadGroup(ctx context.Context) ([]E, error) {
	streams, err := c.client.XReadGroup(ctx, c.groupReadArgs).Result()
	if err != nil {
		return nil, err
	}

	events := make([]E, 0, len(streams[0].Messages))
	for _, event := range streams[0].Messages {
		convertedEvent, err := c.converter.To(event.ID, event.Values)
		if err != nil {
			log.Printf("converter error %w\n", err)
		} else {
			events = append(events, convertedEvent)
		}
	}

	return events, nil
}

func (c StreamClient[E]) Pending() {

}

func (c StreamClient[E]) Claim() {

}

func (c StreamClient[E]) Ack() {

}

func (c StreamClient[E]) Del() {

}
