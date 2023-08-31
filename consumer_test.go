package goxstreams_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/khv1one/goxstreams"
	"github.com/redis/go-redis/v9"
)

type ConsumerEvent struct {
	RedisID string
	Foo     string
	Bar     int
}

type ConsumerConverter[E any] struct{}

func (c ConsumerConverter[E]) To(id string, event map[string]interface{}) (ConsumerEvent, error) {
	result := ConsumerEvent{}

	foo, ok := event["foo"].(string)
	if !ok {
		return result, errors.New("error convert to EventStruct, foo is not exist")
	}

	barStr, ok := event["bar"].(string)
	if !ok {
		return result, errors.New("error convert to EventStruct, bar is not exist")
	}
	bar, err := strconv.Atoi(barStr)
	if err != nil {
		return result, err
	}

	result.RedisID = id
	result.Foo = foo
	result.Bar = bar

	return result, nil
}

type Worker[E any] struct {
	Name string
}

func (w Worker[E]) Process(event ConsumerEvent) error {
	fmt.Printf("read event from %v: %v, worker: %v\n", "mystream", event, w.Name)

	return nil
}

func (w Worker[E]) ProcessBroken(broken map[string]interface{}) error {
	fmt.Printf("read broken event from %v: %v, worker: %v\n", "mystream", broken, w.Name)

	return nil
}

func (w Worker[E]) ProcessDead(dead ConsumerEvent) error {
	fmt.Printf("event %v from stream %v is dead!, worker: %v\n", dead.RedisID, "mystream", w.Name)

	return nil
}

func (c ConsumerConverter[E]) GetRedisID(event ConsumerEvent) string {
	return event.RedisID
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

		myConsumer := goxstreams.NewConsumer[ConsumerEvent](
			redisClient,
			ConsumerConverter[ConsumerEvent]{},
			Worker[ConsumerEvent]{"foo"},
			config,
		)

		return myConsumer
	}

	consumerInit().Run(context.Background())
}
