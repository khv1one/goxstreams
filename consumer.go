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
	Process(ctx context.Context, event RedisMessage[E]) error
	ProcessBroken(ctx context.Context, event RedisBrokenMessage) error
	ProcessDead(ctx context.Context, event RedisMessage[E]) error
}

// Consumer is a wrapper to easily getting messages from redis stream.
type Consumer[E any] struct {
	client     streamClient
	worker     Worker[E]
	errorLog   logger
	config     ConsumerConfig
	convertTo  func(event map[string]interface{}) (*E, error)
	receiveCtx func(ctx context.Context, event map[string]interface{}) context.Context
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

// WithConverter is a Consumer's method for custom data converting.
//
// Since Redis Streams messages are limited to a flat structure, we have 2 options available:
//   - flat Example: ("foo_key", "foo_val", "bar_key", "bar_val");
//   - nested json or proto into one key ("key", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
//   - or combination ("foo_key", "foo_val", "foobar", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
func (c Consumer[E]) WithConverter(
	convertTo func(event map[string]interface{}) (*E, error),
) Consumer[E] {
	c.convertTo = convertTo

	return c
}

// WithCtxReceiver is a method for receiving context fields
//
// Looks like "ctx_field" "{"any_info":"info", "trace_id": "my_trace_id"}" in redis event
func (c Consumer[E]) WithCtxReceiver(
	receiveCtx func(ctx context.Context, event map[string]interface{}) context.Context,
) Consumer[E] {
	c.receiveCtx = receiveCtx

	return c
}

// Run is a method to start processing messages from redis stream.
//
// This method will start two processes: xreadgroup and xpending + xclaim.
// To stop - just cancel the context
func (c Consumer[E]) Run(ctx context.Context) {
	stop := make(chan struct{})

	go func() {
		<-ctx.Done()
		close(stop)
	}()

	processCtx := context.Background()

	//FanIn
	in := c.merge(c.runEventsRead(processCtx, stop), c.runFailEventsRead(processCtx, stop))

	//FanOut
	events, deads, brokens := c.runConvertAndSplit(ctx, in)

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

				for _, event := range *eventsPtr {
					out <- event
				}

				c.client.xMessageSliceToPool(eventsPtr)
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

				for _, event := range *eventsPtr {
					out <- event
				}
				c.client.xMessageSliceToPool(eventsPtr)

				<-ticker.C
			}
		}
	}()

	return out
}

func (c Consumer[E]) merge(cs ...<-chan xRawMessage) <-chan xRawMessage {
	var wg sync.WaitGroup
	wg.Add(len(cs))

	out := make(chan xRawMessage)
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
	ctx context.Context, in <-chan xRawMessage,
) (<-chan redisMessageCtx[E], <-chan redisMessageCtx[E], <-chan redisBrokenMessageCtx) {
	outEvents := make(chan redisMessageCtx[E])
	outDeads := make(chan redisMessageCtx[E])
	outBrokens := make(chan redisBrokenMessageCtx)

	toChannels := func() {
		defer func() {
			close(outEvents)
			close(outDeads)
			close(outBrokens)
		}()

		for event := range in {
			messageCtx := context.WithoutCancel(ctx)
			if c.receiveCtx != nil {
				messageCtx = c.receiveCtx(messageCtx, event.Values)
			}

			convertedEvent, err := c.convertEvent(event)
			if err != nil {
				outBrokens <- redisBrokenMessageCtx{
					messageCtx,
					newRedisBrokenMessage(event.ID, event.RetryCount, event.Values, err),
				}

				continue
			}

			if event.RetryCount > c.config.MaxRetries {
				outDeads <- redisMessageCtx[E]{
					messageCtx,
					newRedisMessage(event.ID, event.RetryCount, *convertedEvent),
				}

				continue
			}

			outEvents <- redisMessageCtx[E]{
				messageCtx,
				newRedisMessage(event.ID, event.RetryCount, *convertedEvent),
			}
		}
	}

	go toChannels()

	return outEvents, outDeads, outBrokens
}

func (c Consumer[E]) runProccessing(
	ctx context.Context, stream, deadStream <-chan redisMessageCtx[E], brokenStream <-chan redisBrokenMessageCtx,
) (<-chan string, <-chan string) {
	var inChannelsWg sync.WaitGroup
	inChannelsWg.Add(3)

	var processingWg sync.WaitGroup

	sem := semaphore.NewWeighted(c.config.MaxConcurrency)

	ackChan := make(chan string, c.config.MaxConcurrency)
	delChan := make(chan string, c.config.MaxConcurrency)

	go func() {
		for message := range stream {
			c.safeAcquire(ctx, sem, &processingWg)
			go c.processEvent(message.ctx, message.body, sem, &processingWg, ackChan, delChan)
		}

		inChannelsWg.Done()
	}()

	go func() {
		for message := range brokenStream {
			c.safeAcquire(ctx, sem, &processingWg)
			go c.processBroken(context.WithoutCancel(ctx), message.body, sem, &processingWg, delChan)
		}

		inChannelsWg.Done()
	}()

	go func() {
		for message := range deadStream {
			c.safeAcquire(ctx, sem, &processingWg)
			go c.processDead(message.ctx, message.body, sem, &processingWg, delChan)
		}

		inChannelsWg.Done()
	}()

	go func() {
		inChannelsWg.Wait()
		processingWg.Wait()
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

	err := c.worker.Process(ctx, message)
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

	err := c.worker.ProcessBroken(ctx, broken)
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

	err := c.worker.ProcessDead(ctx, dead)
	if err != nil {
		c.errorLog.print(fmt.Sprintf("id: %v, error: %v", dead.ID, err))
		return
	}

	delChan <- dead.ID
}

func (c Consumer[E]) runFinishingBatching(
	ctx context.Context, idsChan <-chan string, f func(ctx context.Context, ids []string) error,
) {
	var m sync.Mutex
	stopTimer := make(chan struct{})
	ticker := time.NewTicker(100 * time.Millisecond)

	ids := make([]string, c.config.MaxConcurrency)
	i := 0

	fCall := func() {
		m.Lock()
		if i > 0 {
			if err := f(ctx, ids[:i]); err != nil {
				c.errorLog.err(err)
			}

			i = 0
			ticker.Reset(100 * time.Millisecond)
		}
		m.Unlock()
	}

	go func() {
		for id := range idsChan {
			ids[i] = id
			i += 1

			if int64(i) == c.config.MaxConcurrency {
				fCall()
			}
		}

		fCall()

		close(stopTimer)
	}()

	go func() {
		for {
			select {
			case <-stopTimer:
				return

			case <-ticker.C:
				fCall()
			}
		}
	}()
}

func (c Consumer[E]) convertEvent(raw xRawMessage) (*E, error) {
	convertedEvent, err := c.convertTo(raw.Values)
	if err != nil {
		return convertedEvent, err
	}

	return convertedEvent, nil
}

func (c Consumer[E]) safeAcquire(ctx context.Context, sem *semaphore.Weighted, wg *sync.WaitGroup) {
	wg.Add(1)
	err := sem.Acquire(ctx, 1)

	if err != nil {
		c.errorLog.fatal(fmt.Sprintf("can`t acquire semaphore: %v\n", err))
	}
}
