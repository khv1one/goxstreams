package goxstreams_test

import (
	"context"

	"github.com/khv1one/goxstreams"
	"github.com/redis/go-redis/v9"
)

type ProducerEvent struct {
	Foo string
	Bar int
}

type ProduceConverter[E any] struct{}

func (c ProduceConverter[E]) From(event ProducerEvent) map[string]interface{} {
	result := make(map[string]interface{})

	result["foo"] = event.Foo
	result["bar"] = event.Bar

	return result
}

func ExampleProducer_Produce() {
	converter := ProduceConverter[ProducerEvent]{}
	producer := goxstreams.NewProducer[ProducerEvent](redis.NewClient(&redis.Options{Addr: "localhost:6379"}), converter)

	event := ProducerEvent{"foo", 1}

	_ = producer.Produce(context.Background(), event, "mystream")
}
