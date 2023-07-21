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

// TODO remove E from client and remove Client interface
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
	eventStream := make(chan E)
	failEventStream := make(chan map[string]interface{})
	deadEventStream := make(chan E)

	go c.runEventsRead(ctx, eventStream, failEventStream)
	go c.runFailEventsRead(ctx, eventStream, failEventStream, deadEventStream)

	for w := 1; w <= 100; w++ {
		go c.runProccessing(ctx, eventStream, failEventStream, deadEventStream)
	}
}

func (c Consumer[E]) runEventsRead(ctx context.Context, stream chan E, brokenStream chan map[string]interface{}) {
	defer func() {
		close(stream)
		close(brokenStream)
	}()

	for {
		events, brokens, err := c.client.ReadEvents(ctx, c.converter.To)

		if err != nil {
			c.errorLog.Print(err)
			wait(10)
			continue
		}

		if len(events) > 0 {
			go func() {
				for _, event := range events {
					stream <- event
				}
			}()
		}

		if len(brokens) > 0 {
			go func() {
				for _, broken := range brokens {
					brokenStream <- broken
				}
			}()
		}
	}
}

func (c Consumer[E]) runFailEventsRead(ctx context.Context, stream chan E, brokenStream chan map[string]interface{}, deadStream chan E) {
	defer func() {
		close(stream)
		close(brokenStream)
		close(deadStream)
	}()

	for {
		events, deads, brokens, err := c.client.ReadFailEvents(ctx, c.converter.To, c.maxRetries)
		if err != nil {
			c.errorLog.Print(err)
			wait(10)
			continue
		}

		if len(events) > 0 {
			go func() {
				for _, event := range events {
					stream <- event
				}
			}()
		}

		if len(brokens) > 0 {
			go func() {
				for _, broken := range brokens {
					brokenStream <- broken
				}
			}()
		}

		if len(deads) > 0 {
			go func() {
				for _, dead := range deads {
					deadStream <- dead
				}
			}()
		}

		wait(100)
	}
}

func (c Consumer[E]) runProccessing(
	ctx context.Context, stream <-chan E, brokenStream <-chan map[string]interface{}, deadStream <-chan E,
) {
	for {
		select {
		case event := <-stream:
			c.processEvent(ctx, event)

		case broken := <-brokenStream:
			c.processBroken(ctx, broken)

		case dead := <-deadStream:
			c.processDead(ctx, dead)

		case <-ctx.Done():
			return
		}
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
