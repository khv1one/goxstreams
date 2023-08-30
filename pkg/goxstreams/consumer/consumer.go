package consumer

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/khv1one/goxstreams/pkg/goxstreams"
	"github.com/khv1one/goxstreams/pkg/goxstreams/client"
)

type Converter[E any] interface {
	To(id string, event map[string]interface{}) (E, error)
	GetRedisID(E) string
}

type Worker[E any] interface {
	Process(event E) error
	ProcessBroken(event map[string]interface{}) error
	ProcessDead(event E) error
}

type Consumer[E any] struct {
	client     client.StreamClient
	converter  Converter[E]
	worker     Worker[E]
	cleaneUp   bool
	skipDead   bool
	maxRetries int64
	errorLog   *log.Logger
}

func NewConsumer[E any](
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
	stop1 := make(chan struct{})
	stop2 := make(chan struct{})

	go func() {
		<-ctx.Done()
		close(stop1)
		close(stop2)
	}()

	//FanIn
	in := c.merge(c.runEventsRead(ctx, stop1), c.runFailEventsRead(ctx, stop2))

	//FanOut
	events, deads, brokens := c.runConvertAndSplit(in)

	c.runProccessing(events, deads, brokens)
}

func (c Consumer[E]) runEventsRead(ctx context.Context, stop <-chan struct{}) <-chan goxstreams.XRawMessage {
	out := make(chan goxstreams.XRawMessage)

	go func() {
		for {
			select {
			case <-stop:
				close(out)
				return

			default:
				events, err := c.client.ReadEvents(ctx)
				if err != nil {
					c.errorLog.Print(err)
					timer := time.NewTimer(10 * time.Millisecond)
					<-timer.C
					continue
				}

				for _, event := range events {
					out <- event
				}
			}
		}
	}()

	return out
}

func (c Consumer[E]) runFailEventsRead(ctx context.Context, stop <-chan struct{}) <-chan goxstreams.XRawMessage {
	out := make(chan goxstreams.XRawMessage)
	ticker := time.NewTicker(100 * time.Millisecond)

	go func() {
		for {
			select {
			case <-stop:
				close(out)
				return

			default:
				events, err := c.client.ReadFailEvents(ctx)
				if err != nil {
					c.errorLog.Print(err)
					<-ticker.C
					continue
				}

				for _, event := range events {
					out <- event
				}

				<-ticker.C
			}
		}
	}()

	return out
}

func (c Consumer[E]) merge(cs ...<-chan goxstreams.XRawMessage) <-chan goxstreams.XRawMessage {
	var wg sync.WaitGroup
	out := make(chan goxstreams.XRawMessage)
	wg.Add(len(cs))

	for _, channel := range cs {
		go func(c <-chan goxstreams.XRawMessage) {
			for n := range c {
				out <- n
			}
			wg.Done()
		}(channel)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func (c Consumer[E]) runConvertAndSplit(
	in <-chan goxstreams.XRawMessage,
) (<-chan E, <-chan E, <-chan map[string]interface{}) {
	outEvents := make(chan E)
	outDeads := make(chan E)
	outBrokens := make(chan map[string]interface{})

	toChannels := func() {
		defer func() {
			close(outEvents)
			close(outDeads)
			close(outBrokens)
		}()

		for event := range in {
			convertedEvent, err := c.convertEvent(event)
			if err != nil {
				event.Values["MessageID"] = event.ID
				event.Values["ErrorMessage"] = err.Error()
				outBrokens <- event.Values
				continue
			}

			if event.RetryCount > c.maxRetries {
				outDeads <- convertedEvent
				continue
			}

			outEvents <- convertedEvent
		}

		return
	}

	go toChannels()

	return outEvents, outDeads, outBrokens
}

func (c Consumer[E]) runProccessing(
	stream, deadStream <-chan E, brokenStream <-chan map[string]interface{},
) {
	ctx := context.Background()
	sem := semaphore.NewWeighted(20)

	go func() {
		for event := range stream {
			c.safeAcquire(ctx, sem)
			go c.processEvent(ctx, sem, event)
		}
	}()

	go func() {
		for event := range brokenStream {
			c.safeAcquire(ctx, sem)
			go c.processBroken(ctx, sem, event)
		}
	}()

	go func() {
		for event := range deadStream {
			c.safeAcquire(ctx, sem)
			go c.processDead(ctx, sem, event)
		}
	}()
}

func (c Consumer[E]) processEvent(ctx context.Context, sem *semaphore.Weighted, event E) {
	defer sem.Release(1)

	err := c.worker.Process(event)
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", c.converter.GetRedisID(event), err)
		return
	}

	err = c.client.Ack(ctx, c.converter.GetRedisID(event))
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", c.converter.GetRedisID(event), err)
		return
	}

	if c.cleaneUp {
		err = c.client.Del(ctx, c.converter.GetRedisID(event))
		if err != nil {
			c.errorLog.Printf("id: %v, error: %v", c.converter.GetRedisID(event), err)
			return
		}
	}
}

func (c Consumer[E]) processBroken(ctx context.Context, sem *semaphore.Weighted, broken map[string]interface{}) {
	defer sem.Release(1)

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

func (c Consumer[E]) processDead(ctx context.Context, sem *semaphore.Weighted, dead E) {
	defer sem.Release(1)

	err := c.worker.ProcessDead(dead)
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", c.converter.GetRedisID(dead), err)
		return
	}

	err = c.client.Del(ctx, c.converter.GetRedisID(dead))
	if err != nil {
		c.errorLog.Printf("id: %v, error: %v", c.converter.GetRedisID(dead), err)
		return
	}
}

func (c Consumer[E]) convertEvent(raw goxstreams.XRawMessage) (E, error) {
	convertedEvent, err := c.converter.To(raw.ID, raw.Values)
	if err != nil {
		return convertedEvent, err
	}

	return convertedEvent, nil
}

func (c Consumer[E]) safeAcquire(ctx context.Context, sem *semaphore.Weighted) {
	err := sem.Acquire(ctx, 1)
	if err != nil {
		c.errorLog.Fatal("can`t acquire semaphore: %v", err)
	}
}
