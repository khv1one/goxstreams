package consumer

import (
	"context"
	"log"
	"os"
	"time"
)

type RedisEvent interface {
	GetRedisID() string
}

type Client[E RedisEvent] interface {
	ReadEvents(ctx context.Context, to func(string, map[string]interface{}) (E, error)) ([]E, []map[string]interface{}, error)
	ReadFailEvents(
		ctx context.Context, to func(string, map[string]interface{}) (E, error), maxRetries int64,
	) ([]E, []E, []map[string]interface{}, error)
	Ack(ctx context.Context, id string) error
	Del(ctx context.Context, id string) error
}

type Converter[E RedisEvent] interface {
	To(id string, event map[string]interface{}) (E, error)
}

type Worker[E RedisEvent] interface {
	Process(event E) error
	ProcessBroken(event map[string]interface{}) error
	ProcessDead(event E) error
}

type Consumer[E RedisEvent] struct {
	client     Client[E]
	converter  Converter[E]
	worker     Worker[E]
	cleaneUp   bool
	skipDead   bool
	maxRetries int64
	errorLog   *log.Logger
}

func NewConsumer[E RedisEvent](
	client Client[E], converter Converter[E], worker Worker[E], maxRetries int64,
) Consumer[E] {
	errorLog := log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile)
	return Consumer[E]{
		client:     client,
		converter:  converter,
		worker:     worker,
		cleaneUp:   false,
		skipDead:   false,
		maxRetries: maxRetries,
		errorLog:   errorLog,
	}
}

func (c Consumer[E]) Run(ctx context.Context) {
	go c.runEventsRead(ctx)
	go c.runFailEventsRead(ctx)
}

func (c Consumer[E]) runEventsRead(ctx context.Context) {
	for {
		events, brokens, err := c.client.ReadEvents(ctx, c.converter.To)

		if err != nil {
			c.errorLog.Print(err)
			wait(10)
			continue
		}

		c.runEventProccessing(ctx, events)
		c.runBrokenProccessing(ctx, brokens)
	}
}

func (c Consumer[E]) runFailEventsRead(ctx context.Context) {
	for {
		events, deads, brokens, err := c.client.ReadFailEvents(ctx, c.converter.To, c.maxRetries)
		if err != nil {
			c.errorLog.Print(err)
			wait(10)
			continue
		}

		c.runEventProccessing(ctx, events)
		c.runBrokenProccessing(ctx, brokens)
		c.runDeadProccessing(ctx, deads)

		wait(10)
	}
}

func (c Consumer[E]) runEventProccessing(ctx context.Context, events []E) {
	for _, event := range events {
		c.processEvent(ctx, event)
	}
}

func (c Consumer[E]) runBrokenProccessing(ctx context.Context, brokens []map[string]interface{}) {
	for _, event := range brokens {
		c.processBroken(ctx, event)
	}
}

func (c Consumer[E]) runDeadProccessing(ctx context.Context, deads []E) {
	for _, event := range deads {
		c.processDead(ctx, event)
	}
}

func (c Consumer[E]) processEvent(ctx context.Context, event E) {
	err := c.worker.Process(event)
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", event.GetRedisID(), err)
		return
	}

	err = c.client.Ack(ctx, event.GetRedisID())
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", event.GetRedisID(), err)
		return
	}

	if c.cleaneUp {
		err := c.client.Del(ctx, event.GetRedisID())
		if err != nil {
			c.errorLog.Printf("id: %v, error: %v", event.GetRedisID(), err)
			return
		}
	}
}

func (c Consumer[E]) processBroken(ctx context.Context, broken map[string]interface{}) {
	err := c.worker.ProcessBroken(broken)
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", broken["RedisID"], err)
		return
	}

	err = c.client.Del(ctx, broken["RedisID"].(string))
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", broken["RedisID"], err)
		return
	}
}

func (c Consumer[E]) processDead(ctx context.Context, dead E) {
	err := c.worker.ProcessDead(dead)
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", dead.GetRedisID(), err)
		return
	}

	err = c.client.Del(ctx, dead.GetRedisID())
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", dead.GetRedisID(), err)
		return
	}
}

func wait(durationMillis int) {
	time.Sleep(time.Duration(durationMillis) * time.Millisecond)
}
