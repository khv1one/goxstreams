package consumer

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/khv1one/goxstreams/pkg/goxstreams"

	"github.com/khv1one/goxstreams/pkg/goxstreams/client"
)

type RedisEvent interface {
	GetRedisID() string
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
	client     client.StreamClient
	converter  Converter[E]
	worker     Worker[E]
	cleaneUp   bool
	skipDead   bool
	maxRetries int64
	errorLog   *log.Logger
}

func NewConsumer[E RedisEvent](
	client client.StreamClient, converter Converter[E], worker Worker[E], maxRetries int64,
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
	rawEvents := make(chan goxstreams.XRawMessage, 50)
	events := make(chan E)
	brokens := make(chan map[string]interface{})
	deads := make(chan E)

	go c.runEventsRead(rawEvents)
	go c.runFailEventsRead(rawEvents)

	go c.runConverting(rawEvents, events, deads, brokens)

	for w := 1; w <= 100; w++ {
		go c.runProccessing(ctx, events, deads, brokens)
	}
}

func (c Consumer[E]) runEventsRead(stream chan goxstreams.XRawMessage) {
	for {
		ctx := context.Background()
		events, err := c.client.ReadEvents(ctx)
		if err != nil {
			c.errorLog.Print(err)
			wait(10)
			continue
		}

		for _, event := range events {
			stream <- event
		}
	}
}

func (c Consumer[E]) runFailEventsRead(stream chan goxstreams.XRawMessage) {
	for {
		ctx := context.Background()
		events, err := c.client.ReadFailEvents(ctx)
		if err != nil {
			c.errorLog.Print(err)
			wait(10)
			continue
		}

		for _, event := range events {
			stream <- event
		}

		wait(100)
	}
}

func (c Consumer[E]) runConverting(
	in chan goxstreams.XRawMessage,
	outEvents chan E,
	outDeads chan E,
	outBrokens chan map[string]interface{},
) {
	defer func() {
		close(in)
		close(outEvents)
		close(outDeads)
		close(outBrokens)
	}()

	for {
		event := <-in
		convertedEvent, err := c.convertEvent(event)
		if err != nil {
			event.Values["MessageID"] = event.ID
			event.Values["ErrorMessage"] = err.Error()
			outBrokens <- event.Values
		}

		if event.RetryCount > c.maxRetries {
			outDeads <- convertedEvent
		}

		outEvents <- convertedEvent
	}
}

func (c Consumer[E]) runProccessing(
	ctx context.Context, stream, deadStream <-chan E, brokenStream <-chan map[string]interface{},
) {
	for {
		select {
		case event := <-stream:
			c.processEvent(event)

		case broken := <-brokenStream:
			c.processBroken(broken)

		case dead := <-deadStream:
			c.processDead(dead)

		case <-ctx.Done():
			return
		}
	}
}

func (c Consumer[E]) processEvent(event E) {
	ctx := context.Background()

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

func (c Consumer[E]) processBroken(broken map[string]interface{}) {
	ctx := context.Background()

	err := c.worker.ProcessBroken(broken)
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", broken["ID"], err)
		return
	}

	err = c.client.Del(ctx, broken["ID"].(string))
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", broken["ID"], err)
		return
	}
}

func (c Consumer[E]) processDead(dead E) {
	ctx := context.Background()

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

func (c Consumer[E]) convertEvent(raw goxstreams.XRawMessage) (E, error) {
	convertedEvent, err := c.converter.To(raw.ID, raw.Values)
	if err != nil {
		return convertedEvent, err
	}

	return convertedEvent, nil
}
