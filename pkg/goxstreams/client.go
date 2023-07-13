package goxstreams

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisEvent interface {
	GetRedisID() string
}

type StreamClient[E RedisEvent] struct {
	client        *redis.Client
	groupReadArgs *redis.XReadGroupArgs
	pendingArgs   *redis.XPendingExtArgs
	claimArgs     *redis.XClaimArgs
}

type Params struct {
	Stream   string
	Group    string
	Consumer string
	Batch    int64
	NoAck    bool
}

func NewClient[E RedisEvent](client *redis.Client, params Params) StreamClient[E] {
	groupReadArgs := &redis.XReadGroupArgs{
		Streams:  []string{params.Stream, ">"},
		Group:    params.Group,
		Consumer: params.Consumer,
		Count:    params.Batch,
		NoAck:    params.NoAck,
	}

	pendingArgs := &redis.XPendingExtArgs{
		Stream:   params.Stream,
		Group:    params.Group,
		Consumer: params.Consumer,
		Count:    params.Batch,
		Start:    "-",
		End:      "+",
		Idle:     time.Duration(250) * time.Millisecond,
	}

	claimArgs := &redis.XClaimArgs{
		Stream:   params.Stream,
		Group:    params.Group,
		Consumer: params.Consumer,
		MinIdle:  time.Duration(250) * time.Millisecond,
	}

	streamClient := StreamClient[E]{
		client:        client,
		groupReadArgs: groupReadArgs,
		pendingArgs:   pendingArgs,
		claimArgs:     claimArgs,
	}

	return streamClient
}

func (c StreamClient[E]) Add(
	ctx context.Context, stream string, event E, from func(event E) map[string]interface{},
) error {
	_, err := c.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: from(event)}).Result()

	return err
}

func (c StreamClient[E]) ReadEvents(
	ctx context.Context, to func(string, map[string]interface{}) (E, error),
) ([]E, []map[string]interface{}, error) {
	streams, err := c.client.XReadGroup(ctx, c.groupReadArgs).Result()
	if err != nil {
		return nil, nil, err
	}

	events := make([]E, 0, len(streams[0].Messages))
	brokens := make([]map[string]interface{}, 0)
	for _, event := range streams[0].Messages {
		convertedEvent, err := to(event.ID, event.Values)
		if err != nil {
			event.Values["RedisID"] = event.ID
			brokens = append(brokens, event.Values)
			continue
		}

		events = append(events, convertedEvent)

	}

	return events, brokens, nil
}

func (c StreamClient[E]) ReadFailEvents(
	ctx context.Context, to func(string, map[string]interface{}) (E, error), maxRetries int64,
) ([]E, []E, []map[string]interface{}, error) {
	pendingsIds, deadIds, err := c.pending(ctx, maxRetries)
	if err != nil {
		return nil, nil, nil, err
	}

	var events []E
	var deadEvents []E
	var brokens []map[string]interface{}
	var deadBrokens []map[string]interface{}
	if len(pendingsIds) > 0 {
		rawEvents, err := c.claim(ctx, pendingsIds)
		if err != nil {
			return nil, nil, nil, err
		}

		events, brokens = c.convertEvents(rawEvents, to)
	}

	if len(deadIds) > 0 {
		rawDeadEvents, err := c.claim(ctx, deadIds)
		if err != nil {
			return nil, nil, nil, err
		}

		deadEvents, deadBrokens = c.convertEvents(rawDeadEvents, to)
	}

	return events, deadEvents, append(brokens, deadBrokens...), nil
}

func (c StreamClient[E]) Ack(ctx context.Context, id string) error {
	res := c.client.XAck(ctx, c.groupReadArgs.Streams[0], c.groupReadArgs.Group, id)

	return res.Err()
}

func (c StreamClient[E]) Del(ctx context.Context, id string) error {
	res := c.client.XDel(ctx, c.groupReadArgs.Streams[0], id)

	return res.Err()
}

func (c StreamClient[E]) claim(ctx context.Context, ids []string) ([]redis.XMessage, error) {
	args := c.claimArgs
	args.Messages = ids
	rawEvents, err := c.client.XClaim(ctx, args).Result()

	return rawEvents, err
}

func (c StreamClient[E]) pending(ctx context.Context, maxRetries int64) ([]string, []string, error) {
	pendings, err := c.client.XPendingExt(ctx, c.pendingArgs).Result()
	if err != nil {
		return nil, nil, err
	}

	if len(pendings) == 0 {
		return nil, nil, nil
	}

	pendingsIds := make([]string, 0, len(pendings))
	deadIds := make([]string, 0, len(pendings))
	for _, pendingEvent := range pendings {
		if pendingEvent.RetryCount < maxRetries {
			pendingsIds = append(pendingsIds, pendingEvent.ID)
		} else {
			deadIds = append(deadIds, pendingEvent.ID)
		}
	}

	return pendingsIds, deadIds, nil
}

func (c StreamClient[E]) convertEvents(
	rawEvents []redis.XMessage, to func(string, map[string]interface{}) (E, error),
) ([]E, []map[string]interface{}) {
	brokens := make([]map[string]interface{}, 0)
	events := make([]E, 0, len(rawEvents))

	for _, rawEvent := range rawEvents {
		convertedEvent, err := to(rawEvent.ID, rawEvent.Values)
		if err != nil {
			rawEvent.Values["RedisID"] = rawEvent.ID
			rawEvent.Values["Err"] = err.Error()
			brokens = append(brokens, rawEvent.Values)
			continue
		}

		events = append(events, convertedEvent)
	}

	return events, brokens
}
