# goxstreams [![GoDoc](https://godoc.org/github.com/khv1one/goxstreams?status.png)](https://pkg.go.dev/github.com/khv1one/goxstreams)

### Based on the [go-redis](https://github.com/redis/go-redis)  library [![Go-Redis](https://cdn4.iconfinder.com/data/icons/redis-2/1451/Untitled-2-36.png)](https://github.com/redis/go-redis)
goxstreams lets you to post and processes messages asynchronously using Redis Streams

- Reliable - don't lose messages even if your process crashes
- If message processing fails, it will be repeated the specified number of times after the specified time.
- Horizontally scalable - specify the number of goroutines in parallel running applications
- Don't describe low-level interaction - focus on business logic

##An example code can be found here
[click](https://github.com/khv1one/go-redis-streams-example)

## Describe the business model

 - Describe the model that we want to put in the stream

```go
package app

type Event struct {
	RedisID string
	Foo       string
	Bar       int
}
```

- Describe how this model is converted FROM a structure to a hash and TO a structure from a hash
- Implement the method returning redis id

```go
type Converter[E any] struct{}

func NewConverter[E any]() Converter[E] {
	return Converter[E]{}
}

func (c Converter[E]) From(event Event) map[string]interface{} {
	result := make(map[string]interface{})

	result["foo"] = event.Foo
	result["bar"] = event.Bar

	return result
}

func (c Converter[E]) To(id string, event map[string]interface{}) (Event, error) {
	result := Event{}

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

func (c Converter[E]) GetRedisID(event Event) string {
	return event.RedisID
}
```
## Producing messages

###Initialize your application:
- create go-redis client
- create converter object
- create producer

```go
package main

import (
"context"
"fmt"
"math/rand"
"time"

"example/app"

"github.com/khv1one/goxstreams"
"github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()

    converter := app.Converter[app.Event]{}
    producer := goxstreams.NewProducer[app.Event](redis.NewClient(&redis.Options{Addr: "localhost:6379"}), converter)
    go write(producer, ctx)

    fmt.Println("Producer started")
    <-ctx.Done()
}

func write(producer goxstreams.Producer[app.Event], ctx context.Context) {
    for {
        event := app.Event{Foo: "foo", Bar: rand.Intn(1000)}

        err := producer.Produce(ctx, event, "mystream")
        fmt.Printf("produced %v\n", event)
        if err != nil {
            fmt.Printf("write error %v\n", err)
            time.Sleep(time.Second)
            continue
        }

        time.Sleep(100 * time.Millisecond)
    }
}
```
You can use one producer to publish to different streams


## Processing messages
###Describe worker
```go
package app

import (
"errors"
"fmt"
"math/rand"
"time"
)

type Worker[E any] struct {
    Name string
}

func NewWorker[E any](name string) Worker[E] {
    return Worker[E]{Name: name}
}

func (w Worker[E]) Process(event Event) error {
    time.Sleep(1000 * time.Millisecond)

    a := rand.Intn(20)
    if a == 0 {
        return errors.New("rand error")
    } else {
        fmt.Printf("read event from %v: %v, worker: %v\n", "mystream", event, w.Name)
    }

    return nil
}

func (w Worker[E]) ProcessBroken(broken map[string]interface{}) error {
    fmt.Printf("read broken event from %v: %v, worker: %v\n", "mystream", broken, w.Name)

    return nil
}

func (w Worker[E]) ProcessDead(dead Event) error {
    fmt.Printf("event %v from stream %v is dead!, worker: %v\n", dead.RedisID, "mystream", w.Name)

    return nil
}
```
you need to implement 3 methods:
- normal message processing (including reprocessing)
- processing of messages that could not be converted to the model (for example, put them in the database for further investigation)
- processing messages, the number of repetitions of which exceeded the number specified in the config

###Initialize your application:
- create go-redis client
- create converter object
- create worker object
- create consumer config
- create consumer
- run consumer

```go
package main

import (
"context"
"fmt"
"time"

"example/app"

"github.com/khv1one/goxstreams"
"github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    consumerCtx, _ := context.WithCancel(ctx)

    consumerInit().Run(consumerCtx)
    fmt.Println("Consumer Started")

    <-ctx.Done()
}

func consumerInit() goxstreams.Consumer[app.Event] {
    redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    config := goxstreams.ConsumerConfig{
        Stream:         	   "mystream",
        Group:          	    "mygroup",
        ConsumerName:   "consumer",
        BatchSize:      	  100,
        MaxConcurrency:  5000,
        NoAck:          	   false,
        MaxRetries:     	 3,
        CleaneUp:       	  false,
        FailReadTime:   	1000 * time.Millisecond,
        FailIdle:       		  5000 * time.Millisecond,
    }

    myConsumer := goxstreams.NewConsumer[app.Event](
    redisClient,
    app.NewConverter[app.Event](),
    app.NewWorker[app.Event]("foo"),
    config,
    )

    return myConsumer
}

```
### Config description
- Stream
--the name of the stream from which we read messages
- Group
--each group processes messages independently of the other
- ConsumerName
--client name in the group, may not be unique
- BatchSize
--the size of messages read from the stream per request
- MaxConcurrency
--maximum number of message processing goroutines
- NoAck
--when true - messages will not be reprocessed if there was an error
- MaxRetries
--the number of times the message will be reprocessed on errors
- CleaneUp
--automatic deletion of messages after successful processing
- FailReadTime
--Failed messages read interval
- FailIdle
--The time after which the message will be considered corrupted

## Benchmarks
WIP
