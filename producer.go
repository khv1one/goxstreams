package goxstreams

import (
	"context"
	"encoding/json"
)

// Producer is a wrapper to easily produce messages to redis stream.
type Producer[E any] struct {
	client      streamClient
	convertFrom func(event *E) (map[string]interface{}, error)
	transmitCtx func(ctx context.Context) (string, map[string]string)
}

// NewProducer is a constructor Producer struct.
func NewProducer[E any](client RedisClient) Producer[E] {
	streamClient := newClient(client, clientParams{})

	return Producer[E]{client: streamClient, convertFrom: convertFrom[*E]}
}

// WithConverter is a Producer's method for custom data converting.
//
// Since Redis Streams messages are limited to a flat structure, we have 2 options available:
//   - flat Example: ("foo_key", "foo_val", "bar_key", "bar_val");
//   - nested json or proto into one key ("key", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
//   - or combination ("foo_key", "foo_val", "foobar", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
func (p Producer[E]) WithConverter(convertFrom func(event *E) (map[string]interface{}, error)) Producer[E] {
	p.convertFrom = convertFrom

	return p
}

// WithCtxTransmmiter is a method for passing context fields
//
// Looks like "ctx_field" "{"any_info":"info", "trace_id": "my_trace_id"}" in redis event
func (p Producer[E]) WithCtxTransmmiter(transmitCtx func(ctx context.Context) (string, map[string]string)) Producer[E] {
	p.transmitCtx = transmitCtx

	return p
}

// Produce method for push message to redis stream.
//
// With default converter, redis message will be like:
//   - "xadd" "mystream" "*" "body" "{\"Message\":\"message\",\"Name\":\"name\",\"Foo\":712,\"Bar\":947}"
func (p Producer[E]) Produce(ctx context.Context, event E, stream string) error {
	eventData, err := p.convertFrom(&event)
	if err != nil {
		return err
	}

	if p.transmitCtx != nil {
		ctxFieldKey, ctxMap := p.transmitCtx(ctx)
		ctxBytes, err := json.Marshal(ctxMap)
		if err != nil {
			return err
		}
		eventData[ctxFieldKey] = ctxBytes
	}

	return p.client.add(ctx, stream, eventData)
}

func (p Producer[E]) ProduceBatch(ctx context.Context, events []E, stream string) error {
	converted := make([]map[string]interface{}, 0, len(events))

	var ctxFieldKey string
	var ctxMap map[string]string
	if p.transmitCtx != nil {
		ctxFieldKey, ctxMap = p.transmitCtx(ctx)
	}

	for _, event := range events {
		eventData, err := p.convertFrom(&event)
		if err != nil {
			continue
		}

		if p.transmitCtx != nil {
			ctxBytes, err := json.Marshal(ctxMap)
			if err != nil {
				return err
			}
			eventData[ctxFieldKey] = ctxBytes
		}

		converted = append(converted, eventData)
	}

	return p.client.addBatch(ctx, stream, converted)
}
