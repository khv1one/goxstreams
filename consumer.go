// Package goxstreams lets you to post and processes messages asynchronously using Redis Streams
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
	client RedisClient, worker Worker[E], config ConsumerConfig,
) Consumer[E] {
	streamClient := newClient(client, clientParams{
		Stream:   config.Stream,
		Group:    config.Group,
		Consumer: config.ConsumerName,
		Batch:    config.BatchSize,
		NoAck:    config.NoAck,
		Idle:     config.FailIdle,
	})

	return Consumer[E]{
		client:    streamClient,
		worker:    worker,
		errorLog:  newLogger(),
		config:    config,
		convertTo: convertTo[*E],
	}
}

// NewConsumerWithConverter is a constructor Consumer struct with custom convert.
//
// Since Redis Streams messages are limited to a flat structure, we have 2 options available:
//   - flat Example: ("foo_key", "foo_val", "bar_key", "bar_val");
//   - nested json or proto into one key ("key", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
//   - or combination ("foo_key", "foo_val", "foobar", "{"foobar": {"foo_key": "foo_val", "bar_key": "bar_val"}}")
func NewConsumerWithConverter[E any](
	client RedisClient, worker Worker[E], convertTo func(event map[string]interface{}) (*E, error), config ConsumerConfig,
) Consumer[E] {
	consumer := NewConsumer(client, worker, config)
	consumer.convertTo = convertTo

	return consumer
}

// Run is a method to start processing messages from redis stream.
func (c Consumer[E]) Run(ctx context.Context) {
	stopRead := make(chan struct{})
	stopReadFail := make(chan struct{})

	go func() {
		<-ctx.Done()
		close(stopRead)
		close(stopReadFail)
	}()

	//FanIn
	in := c.merge(c.runEventsRead(ctx, stopRead), c.runFailEventsRead(ctx, stopReadFail))

	//FanOut
	events, deads, brokens := c.runConvertAndSplit(in)

	c.runProccessing(events, deads, brokens)
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
				events, err := c.client.readEvents(ctx)
				if err != nil {
					c.errorLog.err(err)
					timer := time.NewTimer(c.config.FailReadTime)
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

func (c Consumer[E]) runFailEventsRead(ctx context.Context, stop <-chan struct{}) <-chan xRawMessage {
	out := make(chan xRawMessage)
	ticker := time.NewTicker(c.config.FailReadTime)

	go func() {
		for {
			select {
			case <-stop:
				close(out)
				return

			default:
				events, err := c.client.readFailEvents(ctx)
				if err != nil {
					c.errorLog.err(err)
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
	stream, deadStream <-chan RedisMessage[E], brokenStream <-chan RedisBrokenMessage,
) {
	ctx := context.Background()
	sem := semaphore.NewWeighted(c.config.MaxConcurrency)

	go func() {
		for message := range stream {
			c.safeAcquire(ctx, sem)
			go c.processEvent(ctx, sem, message)
		}
	}()

	go func() {
		for message := range brokenStream {
			c.safeAcquire(ctx, sem)
			go c.processBroken(ctx, sem, message)
		}
	}()

	go func() {
		for message := range deadStream {
			c.safeAcquire(ctx, sem)
			go c.processDead(ctx, sem, message)
		}
	}()
}

func (c Consumer[E]) processEvent(ctx context.Context, sem *semaphore.Weighted, message RedisMessage[E]) {
	defer sem.Release(1)

	err := c.worker.Process(message)
	if err != nil {
		c.errorLog.print(fmt.Sprintf("id: %v, error: %v", message.ID, err))
		return
	}

	if !c.config.NoAck {
		err = c.client.ack(ctx, message.ID)
		if err != nil {
			c.errorLog.print(fmt.Sprintf("id: %v, error: %v", message.ID, err))
			return
		}
	}
	if c.config.CleaneUp {
		err = c.client.del(ctx, message.ID)
		if err != nil {
			c.errorLog.print(fmt.Sprintf("id: %v, error: %v", message.ID, err))
			return
		}
	}
}

func (c Consumer[E]) processBroken(ctx context.Context, sem *semaphore.Weighted, broken RedisBrokenMessage) {
	defer sem.Release(1)

	err := c.worker.ProcessBroken(broken)
	if err != nil {
		c.errorLog.print(fmt.Sprintf("id: %v, error: %v", broken.ID, err))
		return
	}

	err = c.client.del(ctx, broken.ID)
	if err != nil {
		c.errorLog.print(fmt.Sprintf("id: %v, error: %v", broken.ID, err))
		return
	}
}

func (c Consumer[E]) processDead(ctx context.Context, sem *semaphore.Weighted, dead RedisMessage[E]) {
	defer sem.Release(1)

	err := c.worker.ProcessDead(dead)
	if err != nil {
		c.errorLog.print(fmt.Sprintf("id: %v, error: %v", dead.ID, err))
		return
	}

	err = c.client.del(ctx, dead.ID)
	if err != nil {
		c.errorLog.print(fmt.Sprintf("id: %v, error: %v", dead.ID, err))
		return
	}
}

func (c Consumer[E]) convertEvent(raw xRawMessage) (E, error) {
	convertedEvent, err := c.convertTo(raw.Values)
	if err != nil {
		return *convertedEvent, err
	}

	return *convertedEvent, nil
}

func (c Consumer[E]) safeAcquire(ctx context.Context, sem *semaphore.Weighted) {
	err := sem.Acquire(ctx, 1)
	if err != nil {
		c.errorLog.fatal(fmt.Sprintf("can`t acquire semaphore: %v\n", err))
	}
}
