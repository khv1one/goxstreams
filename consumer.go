// Package goxstreams lets you post and processes messages asynchronously using Redis Streams
package goxstreams

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

// Worker is an interface for processing messages from redis stream.
type Worker[E any] interface {
	Process(event RedisMessage[E]) error
	ProcessBroken(event RedisBrokenMessage) error
	ProcessDead(event RedisMessage[E]) error
}

// Consumer is a wrapper to easily getting messages from redis stream.
type Consumer[E any] struct {
	client    streamClient
	worker    Worker[E]
	errorLog  logger
	config    ConsumerConfig
	convertTo func(event map[string]interface{}) (*E, error)
}

// NewConsumer is a constructor Consumer struct.
func NewConsumer[E any](
	ctx context.Context, client RedisClient, worker Worker[E], config ConsumerConfig,
) (Consumer[E], error) {
	config.setDefaults()

	streamClient, err := newClientWithGroupInit(ctx, client, clientParams{
		Stream:       config.Stream,
		Group:        config.Group,
		Consumer:     config.ConsumerName,
		Batch:        config.BatchSize,
		NoAck:        config.NoAck,
		Idle:         config.FailIdle,
		ReadInterval: config.ReadInterval,
	})

	consumer := Consumer[E]{
		client:    streamClient,
		worker:    worker,
		errorLog:  newLogger(),
		config:    config,
		convertTo: convertTo[*E],
	}

	return consumer, err
}

// NewConsumerWithConverter is a constructor Consumer struct with custom convert.
//
// Since Redis Streams messages are limited to a flat structure, we have 2 options available:
//   - flat Example: ("foo_key", "foo_val", "bar_key", "bar_val");
//   - nested json or proto into one key ("key", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
//   - or combination ("foo_key", "foo_val", "foobar", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
func NewConsumerWithConverter[E any](
	ctx context.Context,
	client RedisClient,
	worker Worker[E],
	convertTo func(event map[string]interface{}) (*E, error),
	config ConsumerConfig,
) (Consumer[E], error) {
	consumer, err := NewConsumer(ctx, client, worker, config)
	consumer.convertTo = convertTo

	return consumer, err
}

// Run is a method to start processing messages from redis stream.
//
// This method will start two processes: xreadgroup and xpending + xclaim.
// To stop - just cancel the context
func (c Consumer[E]) Run(ctx context.Context) {
	stopRead := make(chan struct{})
	stopReadFail := make(chan struct{})

	go func() {
		<-ctx.Done()
		close(stopRead)
		close(stopReadFail)
	}()

	processCtx := context.Background()

	//FanIn
	in := c.merge(c.runEventsRead(processCtx, stopRead), c.runFailEventsRead(processCtx, stopReadFail))

	//FanOut
	events, deads, brokens := c.runConvertAndSplit(in)

	ackChan, delChan := c.runProccessing(processCtx, events, deads, brokens)
	c.runFinishingBatching(processCtx, ackChan, c.client.ack)
	c.runFinishingBatching(processCtx, delChan, c.client.del)
}

func (c Consumer[E]) runEventsRead(ctx context.Context, stop <-chan struct{}) <-chan xRawMessage {
	out := make(chan xRawMessage)
	go func() {
		for {
			select {
			case <-stop:
				close(out)
				return

			default:
				eventsPtr, err := c.client.readEvents(ctx)
				if err != nil {
					c.errorLog.err(err)
					<-time.NewTimer(c.config.ReadInterval).C
					continue
				}

				if eventsPtr == nil {
					continue
				}

				events := *eventsPtr
				for _, event := range events {
					out <- event
				}

				c.client.eventPool.xMessagePut(eventsPtr)
			}
		}
	}()

	return out
}

func (c Consumer[E]) runFailEventsRead(ctx context.Context, stop <-chan struct{}) <-chan xRawMessage {
	out := make(chan xRawMessage)
	ticker := time.NewTicker(c.config.ReadInterval)

	go func() {
		for {
			select {
			case <-stop:
				close(out)
				return

			default:
				eventsPtr, err := c.client.readFailEvents(ctx)
				if err != nil {
					c.errorLog.err(err)
					<-ticker.C
					continue
				}

				if eventsPtr == nil {
					<-ticker.C
					continue
				}

				events := *eventsPtr
				for _, event := range events {
					out <- event
				}
				c.client.eventPool.xMessagePut(eventsPtr)

				<-ticker.C
			}
		}
	}()

	return out
}

func (c Consumer[E]) merge(cs ...<-chan xRawMessage) <-chan xRawMessage {
	var wg sync.WaitGroup
	out := make(chan xRawMessage)
	wg.Add(len(cs))

	for _, channel := range cs {
		go func(c <-chan xRawMessage) {
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
	in <-chan xRawMessage,
) (<-chan RedisMessage[E], <-chan RedisMessage[E], <-chan RedisBrokenMessage) {
	outEvents := make(chan RedisMessage[E])
	outDeads := make(chan RedisMessage[E])
	outBrokens := make(chan RedisBrokenMessage)

	toChannels := func() {
		defer func() {
			close(outEvents)
			close(outDeads)
			close(outBrokens)
		}()

		for event := range in {
			convertedEvent, err := c.convertEvent(event)
			if err != nil {
				outBrokens <- newRedisBrokenMessage(event.ID, event.RetryCount, event.Values, err)
				continue
			}

			if event.RetryCount > c.config.MaxRetries {
				outDeads <- newRedisMessage(event.ID, event.RetryCount, convertedEvent)
				continue
			}

			outEvents <- newRedisMessage(event.ID, event.RetryCount, convertedEvent)
		}

		return
	}

	go toChannels()

	return outEvents, outDeads, outBrokens
}

func (c Consumer[E]) runProccessing(
	ctx context.Context, stream, deadStream <-chan RedisMessage[E], brokenStream <-chan RedisBrokenMessage,
) (<-chan string, <-chan string) {
	ackChan := make(chan string, c.config.MaxConcurrency)
	delChan := make(chan string, c.config.MaxConcurrency)

	sem := semaphore.NewWeighted(c.config.MaxConcurrency)
	var processesWg sync.WaitGroup
	processesWg.Add(3)
	var taleWg sync.WaitGroup

	go func() {
		for message := range stream {
			c.safeAcquire(ctx, sem, &taleWg)
			go c.processEvent(ctx, message, sem, &taleWg, ackChan, delChan)
		}

		processesWg.Done()
	}()

	go func() {
		for message := range brokenStream {
			c.safeAcquire(ctx, sem, &taleWg)
			go c.processBroken(ctx, message, sem, &taleWg, delChan)
		}

		processesWg.Done()
	}()

	go func() {
		for message := range deadStream {
			c.safeAcquire(ctx, sem, &taleWg)
			go c.processDead(ctx, message, sem, &taleWg, delChan)
		}

		processesWg.Done()
	}()

	go func() {
		processesWg.Wait()
		taleWg.Wait()
		close(ackChan)
		close(delChan)
	}()

	return ackChan, delChan
}

func (c Consumer[E]) processEvent(
	ctx context.Context, message RedisMessage[E], sem *semaphore.Weighted, wg *sync.WaitGroup, ackChan, delChan chan<- string,
) {
	defer func() {
		sem.Release(1)
		wg.Done()
	}()

	err := c.worker.Process(message)
	if err != nil {
		c.errorLog.print(fmt.Sprintf("id: %v, error: %v", message.ID, err))
		return
	}

	if !c.config.NoAck {
		ackChan <- message.ID
	}
	if c.config.CleaneUp {
		delChan <- message.ID
	}
}

func (c Consumer[E]) processBroken(
	ctx context.Context, broken RedisBrokenMessage, sem *semaphore.Weighted, wg *sync.WaitGroup, delChan chan<- string,
) {
	defer func() {
		sem.Release(1)
		wg.Done()
	}()

	err := c.worker.ProcessBroken(broken)
	if err != nil {
		c.errorLog.print(fmt.Sprintf("id: %v, error: %v", broken.ID, err))
		return
	}

	delChan <- broken.ID
}

func (c Consumer[E]) processDead(
	ctx context.Context, dead RedisMessage[E], sem *semaphore.Weighted, wg *sync.WaitGroup, delChan chan<- string,
) {
	defer func() {
		sem.Release(1)
		wg.Done()
	}()

	err := c.worker.ProcessDead(dead)
	if err != nil {
		c.errorLog.print(fmt.Sprintf("id: %v, error: %v", dead.ID, err))
		return
	}

	delChan <- dead.ID
}

func (c Consumer[E]) runFinishingBatching(ctx context.Context, idsChan <-chan string, f func(ctx context.Context, ids []string) error) {
	var m sync.Mutex
	stopTimer := make(chan struct{})
	ticker := time.NewTicker(100 * time.Millisecond)

	ids := make([]string, c.config.MaxConcurrency)
	i := 0

	fCall := func() {
		if i > 0 {
			if err := f(ctx, ids[:i]); err != nil {
				c.errorLog.err(err)
			}

			i = 0
			ticker.Reset(100 * time.Millisecond)
		}
	}

	go func() {
		for id := range idsChan {
			m.Lock()
			ids[i] = id
			i++

			if int64(i) == c.config.MaxConcurrency {
				fCall()
			}
			m.Unlock()
		}

		m.Lock()
		fCall()
		m.Unlock()

		close(stopTimer)
	}()

	go func() {
		for {
			select {
			case <-stopTimer:
				return

			case <-ticker.C:
				m.Lock()
				fCall()
				m.Unlock()
			}
		}
	}()
}

func (c Consumer[E]) convertEvent(raw xRawMessage) (E, error) {
	convertedEvent, err := c.convertTo(raw.Values)
	if err != nil {
		return *convertedEvent, err
	}

	return *convertedEvent, nil
}

func (c Consumer[E]) safeAcquire(ctx context.Context, sem *semaphore.Weighted, wg *sync.WaitGroup) {
	wg.Add(1)
	err := sem.Acquire(ctx, 1)
	if err != nil {
		c.errorLog.fatal(fmt.Sprintf("can`t acquire semaphore: %v\n", err))
	}
}
