package main

import (
	"context"
	"time"
	//"errors"
	"fmt"

	"github.com/khv1one/goxstreams/internal/app"
	streams "github.com/khv1one/goxstreams/pkg/goxstreams/client"
	"github.com/khv1one/goxstreams/pkg/goxstreams/consumer"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	worker := Worker[app.Event]{"foo"}

	converter := app.Converter[app.Event]{}
	streamClient := streamClientInit()

	consumer := consumer.NewConsumer[app.Event](streamClient, converter, worker, 3)
	consumer.Run(ctx)

	fmt.Printf("Redis started %s\n", "localhost:6379")
	fmt.Scanln()
}

func streamClientInit() streams.StreamClient {
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	streamParams := streams.Params{
		Stream:   "mystream",
		Group:    "mygroup",
		Consumer: "consumer",
		Batch:    500,
	}

	streamClient := streams.NewClient(redisClient, streamParams)

	return streamClient
}

type Worker[E consumer.RedisEvent] struct {
	Name string
}

func (w Worker[E]) Process(event app.Event) error {
	fmt.Printf("read event from %v: %v, worker: %v\n", "mystream", event, w.Name)

	//	a := rand.Intn(2)
	//	if a == 0 {
	//		return errors.New("rand error")
	//	}

	time.Sleep(100 * time.Millisecond)
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
