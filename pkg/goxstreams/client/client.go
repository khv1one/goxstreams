package client

import (
	"context"
	"time"

	"github.com/khv1one/goxstreams/pkg/goxstreams"
	"github.com/redis/go-redis/v9"
)

type StreamClient struct {
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

type pendingMessage struct {
	Id         string
	RetryCount int64
}

func NewClient(client *redis.Client, params Params) StreamClient {
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
		Idle:     time.Duration(5000) * time.Millisecond,
	}

	claimArgs := &redis.XClaimArgs{
		Stream:   params.Stream,
		Group:    params.Group,
		Consumer: params.Consumer,
		MinIdle:  time.Duration(5000) * time.Millisecond,
	}

	streamClient := StreamClient{
		client:        client,
		groupReadArgs: groupReadArgs,
		pendingArgs:   pendingArgs,
		claimArgs:     claimArgs,
	}

	return streamClient
}

func (c StreamClient) Add(
	ctx context.Context, stream string, event map[string]interface{},
) error {
	_, err := c.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: event}).Result()

	return err
}

func (c StreamClient) ReadEvents(ctx context.Context) ([]goxstreams.XRawMessage, error) {
	streams, err := c.client.XReadGroup(ctx, c.groupReadArgs).Result()
	if err != nil {
		return nil, err
	}

	result := make([]goxstreams.XRawMessage, 0, len(streams[0].Messages))
	for _, message := range streams[0].Messages {
		result = append(result, goxstreams.NewXMessage(message.ID, 0, message.Values))
	}

	return result, nil
}

func (c StreamClient) ReadFailEvents(ctx context.Context) ([]goxstreams.XRawMessage, error) {
	pendingsIds, pendingCountByID, pendingErr := c.pending(ctx)
	if pendingErr != nil {
		return nil, pendingErr
	}

	if len(pendingsIds) == 0 {
		return nil, nil
	}

	events, eventsErr := c.claiming(ctx, pendingsIds)
	if eventsErr != nil {
		return nil, eventsErr
	}

	result := make([]goxstreams.XRawMessage, 0, len(pendingsIds))
	for _, event := range events {
		result = append(result, goxstreams.NewXMessage(event.ID, pendingCountByID[event.ID], event.Values))
	}

	return result, nil
}

func (c StreamClient) Ack(ctx context.Context, id string) error {
	res := c.client.XAck(ctx, c.groupReadArgs.Streams[0], c.groupReadArgs.Group, id)

	return res.Err()
}

func (c StreamClient) Del(ctx context.Context, id string) error {
	res := c.client.XDel(ctx, c.groupReadArgs.Streams[0], id)

	return res.Err()
}

func (c StreamClient) pending(ctx context.Context) ([]string, map[string]int64, error) {
	pendings, err := c.client.XPendingExt(ctx, c.pendingArgs).Result()
	if err != nil {
		return nil, nil, err
	}

	if len(pendings) == 0 {
		return nil, nil, nil
	}

	pendingIds := make([]string, 0, len(pendings))
	pendingCountByID := make(map[string]int64, len(pendings))
	for _, pendingEvent := range pendings {
		pendingIds = append(pendingIds, pendingEvent.ID)
		pendingCountByID[pendingEvent.ID] = pendingEvent.RetryCount
	}
	return pendingIds, pendingCountByID, nil
}

func (c StreamClient) claiming(ctx context.Context, ids []string) ([]redis.XMessage, error) {
	if len(ids) > 0 {
		events, err := c.claim(ctx, ids)
		if err != nil {
			return events, err
		}

		return events, nil
	}

	return nil, nil
}

func (c StreamClient) claim(ctx context.Context, ids []string) ([]redis.XMessage, error) {
	args := c.claimArgs
	args.Messages = ids
	rawEvents, err := c.client.XClaim(ctx, args).Result()

	return rawEvents, err
}
