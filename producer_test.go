package goxstreams

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func ExampleProducer_Produce() {
	type ProducerEvent struct {
		Foo string
		Bar int
	}

	producer := NewProducer[ProducerEvent](redis.NewClient(&redis.Options{Addr: "localhost:6379"}))

	_ = producer.Produce(context.Background(), ProducerEvent{"foo", 1}, "mystream")
}
