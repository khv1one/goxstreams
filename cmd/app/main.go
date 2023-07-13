package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/khv1one/goxstreams/internal/app"
	streams "github.com/khv1one/goxstreams/pkg/goxstreams"
	"github.com/khv1one/goxstreams/pkg/goxstreams/consumer"
	"github.com/khv1one/goxstreams/pkg/goxstreams/producer"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	worker := Worker[app.Event]{"foo"}

	converter := app.Converter[app.Event]{}
	streamClient := streamClientInit(ctx, converter)

	consumer := consumer.NewConsumer[app.Event](streamClient, converter, worker, 3)
	producer := producer.NewProducer[app.Event](streamClient, converter)

	consumer.Run(ctx)
	go write(producer, ctx)

	fmt.Printf("Redis started %s\n", "localhost:6379")
	fmt.Scanln()
}

func streamClientInit(ctx context.Context, converter app.Converter[app.Event]) streams.StreamClient[app.Event] {
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	clientParams := streams.Params{
		Stream:   "mystream",
		Group:    "mygroup",
		Consumer: "consumer",
		Batch:    50,
	}

	streamClient := streams.NewClient[app.Event](redisClient, clientParams)

	return streamClient
}

func write(producer producer.Producer[app.Event], ctx context.Context) {
	for {
		event := app.Event{Message: "message", Name: "name", Foo: rand.Intn(1000), Bar: rand.Intn(1000)}

		err := producer.Produce(ctx, event, "mystream")
		fmt.Printf("produced %v\n", event)
		if err != nil {
			log.Printf("write error %v\n", err)
			time.Sleep(time.Second)
			continue
		}

		time.Sleep(100 * time.Millisecond)
	}
}

type Worker[E streams.RedisEvent] struct {
	Name string
}

func (w Worker[E]) Process(event app.Event) error {
	fmt.Printf("read event from %v: %v, worker: %v\n", "mystream", event, w.Name)

	a := rand.Intn(2)
	if a == 0 {
		return errors.New("rand error")
	}

	return nil
}

func (w Worker[E]) ProcessBroken(broken map[string]interface{}) error {
	fmt.Printf("read broken event from %v: %v, worker: %v\n", "mystream", broken, w.Name)

	return nil
}

func (w Worker[E]) ProcessDead(dead app.Event) error {
	fmt.Printf("event %v from stream %v is dead!, worker: %v\n", dead.RedisID, "mystream", w.Name)

	return nil
}
