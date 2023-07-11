package main

import (
	"context"
	"fmt"
	"time"

	"github.com/khv1one/goxstreams/internal/app"
	streams "github.com/khv1one/goxstreams/pkg/goxstreams"
	"github.com/khv1one/goxstreams/pkg/goxstreams/consumer"
	"github.com/khv1one/goxstreams/pkg/goxstreams/producer"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	converter := app.Converter[app.Event]{}
	streamClient := streamClientInit(ctx, converter)

	consumer := consumer.NewConsumer[app.Event](streamClient, converter, process)
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
		event := app.Event{Message: "message", Name: "name", Foo: 777, Bar: 888}

		err := producer.Produce(ctx, event, "mystream")
		//fmt.Printf("produced %v to %v\n", event, key)
		if err != nil {
			fmt.Printf("write error %w\n", err)
			time.Sleep(time.Second)
			continue
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func process(event app.Event) error {
	fmt.Printf("read event from %v: %v\n", "mystream", event)

	return nil
}
