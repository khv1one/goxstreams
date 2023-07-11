package goxstreams

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

type RedisEvent interface {
	GetRedisID() string
}

type StreamClient[E RedisEvent] struct {
	client        *redis.Client
	groupReadArgs *redis.XReadGroupArgs
}

type Params struct {
	Stream   string
	Group    string
	Consumer string
	Batch    int64
}

func NewClient[E RedisEvent](client *redis.Client, params Params) StreamClient[E] {
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
	}

	return streamClient
}

func (c StreamClient[E]) Add(ctx context.Context, stream string, event E, from func(event E) map[string]interface{}) error {
	_, err := c.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: from(event)}).Result()

	return err
}

func (c StreamClient[E]) ReadGroup(ctx context.Context, to func(string, map[string]interface{}) (E, error)) ([]E, error) {
	streams, err := c.client.XReadGroup(ctx, c.groupReadArgs).Result()
	if err != nil {
		return nil, err
	}

	events := make([]E, 0, len(streams[0].Messages))
	for _, event := range streams[0].Messages {
		convertedEvent, err := to(event.ID, event.Values)
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

func (c StreamClient[E]) Ack(ctx context.Context, id string) error {
	res := c.client.XAck(ctx, c.groupReadArgs.Streams[0], c.groupReadArgs.Group, id)

	return res.Err()
}

func (c StreamClient[E]) Del(ctx context.Context, id string) error {
	res := c.client.XDel(ctx, c.groupReadArgs.Streams[0], c.groupReadArgs.Group, id)

	return res.Err()
}
