package goxstreams_test

import (
	"context"
	"fmt"
	"time"

	"github.com/khv1one/goxstreams"
	"github.com/redis/go-redis/v9"
)

type ConsumerEvent struct {
	RedisID string
	Foo     string
	Bar     int
}

type Worker[E any] struct {
	Name string
}

func (w Worker[E]) Process(event goxstreams.RedisMessage[ConsumerEvent]) error {
	fmt.Printf("read event from %s: id: %s, retry: %d, body: %v, worker: %v\n",
		"mystream", event.ID, event.RetryCount, event.Body, w.Name)

	return nil
}

func (w Worker[E]) ProcessBroken(broken goxstreams.RedisBrokenMessage) error {
	fmt.Printf("read broken event from %s: id: %s, retry: %d, body: %v, worker: %v, err: %s\n",
		"mystream", broken.ID, broken.RetryCount, broken.Body, w.Name, broken.Error.Error())

	return nil
}

func (w Worker[E]) ProcessDead(dead goxstreams.RedisMessage[ConsumerEvent]) error {
	fmt.Printf("read from %s is dead!!! id: %s, retry: %d, body: %v, worker: %v\n",
		"mystream", dead.ID, dead.RetryCount, dead.Body, w.Name)

	return nil
}

func ExampleConsumer_Run() {
	consumerInit := func() goxstreams.Consumer[ConsumerEvent] {
		redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

		config := goxstreams.ConsumerConfig{
			Stream:         "mystream",
			Group:          "mygroup",
			ConsumerName:   "consumer",
			BatchSize:      100,
			MaxConcurrency: 5000,
			NoAck:          false,
			MaxRetries:     3,
			CleaneUp:       false,
			FailReadTime:   1000 * time.Millisecond,
			FailIdle:       5000 * time.Millisecond,
		}

		myConsumer := goxstreams.NewConsumer[ConsumerEvent](redisClient, Worker[ConsumerEvent]{"foo"}, config)

		return myConsumer
	}

	consumerInit().Run(context.Background())
}
