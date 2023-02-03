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
	client  *redis.Client
	streams []string
	group   string

	groupReadArgs *redis.XReadGroupArgs

	converter Converter[E]
}

type Params struct {
	Streams  []string
	Group    string
	Consumer string
	Batch    int64
}

func NewClient[E any](client *redis.Client, params Params, converter Converter[E]) StreamClient[E] {
	streamsVal := make([]string, len(params.Streams)*2)
	for i := 0; i < len(params.Streams); i++ {
		streamsVal[i] = params.Streams[i]
		streamsVal[len(params.Streams)-i] = ">"
	}

	groupReadArgs := &redis.XReadGroupArgs{
		Streams:  params.Streams,
		Group:    params.Group,
		Consumer: params.Consumer,
		Count:    params.Batch,
	} //NoAck: true

	streamClient := StreamClient[E]{
		client:        client,
		streams:       streamsVal,
		group:         params.Group,
		groupReadArgs: groupReadArgs,
		converter:     converter,
	}

	return streamClient
}

func (c StreamClient[E]) Add(ctx context.Context, stream string, event E) error {
	_, err := c.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: c.converter.From(event)}).Result()

	return err
}

func (c StreamClient[E]) GroupRead(ctx context.Context) (map[string][]E, error) {
	streams, err := c.client.XReadGroup(ctx, c.groupReadArgs).Result()
	if err != nil {
		return nil, err
	}

	result := make(map[string][]E)
	for _, stream := range streams {
		var events []E

		for _, event := range stream.Messages {
			convertedEvent, err := c.converter.To(event.ID, event.Values)
			if err != nil {
				log.Printf("%v", err)
			}

			events = append(events, convertedEvent)
		}

		result[stream.Stream] = events
	}

	return result, nil
}

func (c StreamClient[E]) Pending() {

}

func (c StreamClient[E]) Claim() {

}

func (c StreamClient[E]) Ack() {

}

func (c StreamClient[E]) Del() {

}
