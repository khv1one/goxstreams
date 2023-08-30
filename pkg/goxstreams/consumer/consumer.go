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
	//FanIn
	in := c.merge(c.runEventsRead(ctx), c.runFailEventsRead(ctx))

	//FanOut
	events, deads, brokens := c.runConvertAndSplit(ctx, in)

	c.runProccessing(ctx, events, deads, brokens)
}

func (c Consumer[E]) runEventsRead(ctx context.Context) <-chan goxstreams.XRawMessage {
	out := make(chan goxstreams.XRawMessage)

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(out)
				return

			default:
				ctx := context.Background()
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

func (c Consumer[E]) runFailEventsRead(ctx context.Context) <-chan goxstreams.XRawMessage {
	out := make(chan goxstreams.XRawMessage)
	ticker := time.NewTicker(100 * time.Millisecond)

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(out)
				return

			default:
				ctx := context.Background()
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
	ctx context.Context,
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

		for {
			select {
			case event := <-in:
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

			case <-ctx.Done():
				return
			}
		}
	}

	go toChannels()

	return outEvents, outDeads, outBrokens
}

func (c Consumer[E]) runProccessing(
	ctx context.Context, stream, deadStream <-chan E, brokenStream <-chan map[string]interface{},
) {
	sem := semaphore.NewWeighted(50)

	for {
		err := sem.Acquire(ctx, 1)
		if err != nil {
			c.errorLog.Fatal("can`t acquire semaphore: %v", err)
		}

		select {
		case event := <-stream:
			go c.processEvent(sem, event)

		case broken := <-brokenStream:
			go c.processBroken(sem, broken)

		case dead := <-deadStream:
			go c.processDead(sem, dead)

		case <-ctx.Done():
			return
		}
	}
}

func (c Consumer[E]) processEvent(sem *semaphore.Weighted, event E) {
	defer sem.Release(1)

	ctx := context.Background()

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

func (c Consumer[E]) processBroken(sem *semaphore.Weighted, broken map[string]interface{}) {
	defer sem.Release(1)

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

func (c Consumer[E]) processDead(sem *semaphore.Weighted, dead E) {
	defer sem.Release(1)

	ctx := context.Background()

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
