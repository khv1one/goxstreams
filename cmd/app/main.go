package main

import (
	"context"
	"fmt"
	"github.com/khv1one/goxstreams/internal/app"
	streams "github.com/khv1one/goxstreams/pkg/goxstreams"
	"github.com/khv1one/goxstreams/pkg/goxstreams/producer"
	"github.com/redis/go-redis/v9"
	"os"
)

func main() {
	ctx := context.Background()

	_, err := clientInit(ctx)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fmt.Printf("Redis started %s", "localhost:6379")
}

func clientInit(ctx context.Context) (streams.StreamClient[app.Event], error) {
	event := app.Event{Message: "message", Name: "name", Foo: 777, Bar: 888}

	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	converter := app.Converter[app.Event]{}

	clientParams := streams.Params{
		Streams:  []string{"mystream1", "mystream2"},
		Group:    "mygroup",
		Consumer: "consumer",
		Batch:    50,
	}

	streamClient := streams.NewClient[app.Event](redisClient, clientParams, converter)
	producer := producer.NewProducer[app.Event](streamClient)

	err := producer.Produce(ctx, event, "mystream")

	return streamClient, err
}
