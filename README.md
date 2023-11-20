# goxstreams [![GoDoc](https://godoc.org/github.com/khv1one/goxstreams?status.png)](https://pkg.go.dev/github.com/khv1one/goxstreams)

### Based on the [go-redis](https://github.com/redis/go-redis)  library [![Go-Redis](https://cdn4.iconfinder.com/data/icons/redis-2/1451/Untitled-2-36.png)](https://github.com/redis/go-redis)

goxstreams lets you to post and processes messages asynchronously using Redis Streams

- Reliable - don't lose messages even if your process crashes
- If message processing fails, it will be repeated the specified number of times after the specified time.
- Horizontally scalable - specify the number of goroutines in parallel running applications
- Don't describe low-level interaction - focus on business logic

## Setting up the environment

All you need is redis

```bash
docker pull redis
```

```bash
docker run -p 6379:6379 --name some-redis -d redis
```

## Describe the business model

- Describe the model that we want to put in the stream

```go
package app

type Event struct {
    Foo      string
    Bar      int
    SubEvent SubEvent
}

type SubEvent struct {
    BarBar string
}
```

## Producing messages

### Initialize your producer application:

- create go-redis client
- create producer

```go
package main

import (
    "context"
    "fmt"

    "github.com/khv1one/goxstreams"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()

    event := app.Event{
        Foo: "foo", Bar: 123,
        SubEvent: app.SubEvent{BarBar: "1234"},
    }

    producer := goxstreams.NewProducer[app.Event](redis.NewClient(&redis.Options{Addr: "localhost:6379"}))

    err := producer.Produce(ctx, event, "mystream")
    if err != nil {
        fmt.Printf("produce %s\n", err.Error())
    }
}
```

You can use one producer to publish to different streams

## Processing messages

### Describe worker

```go
package app

import (
    "context"
    "errors"
    "fmt"
    "math/rand"

    "github.com/khv1one/goxstreams"
)

type Worker[E Event] struct {
    Name string
}

func NewWorker[E Event](name string) Worker[E] {
    return Worker[E]{Name: name}
}

func (w Worker[E]) Process(ctx context.Context, event goxstreams.RedisMessage[E]) error {
    a := rand.Intn(20)
    if a == 0 {
        return errors.New("rand error")
    }

    fmt.Printf("read event from %s: id: %s, retry: %d, body: %v, worker: %v\n",
        "mystream", event.ID, event.RetryCount, event.Body, w.Name)

    return nil
}

func (w Worker[E]) ProcessBroken(ctx context.Context, broken goxstreams.RedisBrokenMessage) error {
    fmt.Printf("read broken event from %s: id: %s, retry: %d, body: %v, worker: %v, err: %s\n",
        "mystream", broken.ID, broken.RetryCount, broken.Body, w.Name, broken.Error.Error())

    return nil
}

func (w Worker[E]) ProcessDead(ctx context.Context, dead goxstreams.RedisMessage[E]) error {
    fmt.Printf("read from %s is dead!!! id: %s, retry: %d, body: %v, worker: %v\n",
        "mystream", dead.ID, dead.RetryCount, dead.Body, w.Name)

    return nil
}
```

You need to implement 3 methods:

- normal message processing (including reprocessing)
- processing of messages that could not be converted to the model (for example, put them in the database for further
  investigation)
- processing messages, the number of retries of which exceeded the number specified in the config

### Initialize your consumer application:

- create go-redis client
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

    "github.com/khv1one/goxstreams/internal/app"

    "github.com/khv1one/goxstreams"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    consumerCtx, _ := context.WithCancel(ctx)

    consumerInit(consumerCtx).Run(consumerCtx)
    fmt.Println("Consumer Started")

    <-ctx.Done()
}

func consumerInit() goxstreams.Consumer[app.Event] {
    redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    config := goxstreams.ConsumerConfig{
        Stream:         "mystream",
        Group:          "mygroup",
        ConsumerName:   "consumer",
        BatchSize:      100,
        MaxConcurrency: 100,
        NoAck:          false,
        MaxRetries:     3,
        CleaneUp:       false,
        ReadInterval:   1000 * time.Millisecond,
        FailIdle:       5000 * time.Millisecond,
    }

    myConsumer, _ := goxstreams.NewConsumer[app.Event](ctx, redisClient, app.NewWorker[app.Event]("name"), config)

    return myConsumer
}
```

### Config description

| Parameter      | description                                                        |
|----------------|--------------------------------------------------------------------|
| Stream         | The name of the stream from which we read messages                 |
| Group          | Each group processes messages independently of the other           |
| ConsumerName   | Client name in the group, may not be unique                        |
| BatchSize      | The size of messages read from the stream per request              |
| MaxConcurrency | Maximum number of message processing goroutines                    |
| NoAck          | When true - messages will not be reprocessed if there was an error |
| MaxRetries     | The number of times the message will be reprocessed on errors      |
| CleaneUp       | Automatic deletion of messages after successful processing         |
| ReadInterval   | Messages read interval                                             |
| FailIdle       | The time after which the message will be considered corrupted      |

# Flexible setup

## Converter

> I'm not happy with encode/json, how can I use a custom converter?

All you need to do is implement the method and pass it on to the consumer or producer

### Producer

```go
package app

func ConvertFrom(event *Event) (map[string]interface{}, error) {
    result := make(map[string]interface{})

    b, err := json.Marshal(event)
    if err != nil {
        return result, err
    }

    result["my_body"] = b
    return result, nil
}
```

#### Initialization

```go
package main

import (
    "context"
    "fmt"

    "github.com/khv1one/goxstreams"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()

    event := app.Event{
        Foo: "foo", Bar: 123,
        SubEvent: app.SubEvent{BarBar: "1234"},
    }

    producer := goxstreams.
        NewProducer[app.Event](redis.NewClient(&redis.Options{Addr: "localhost:6379"})).
        WithConverter(app.ConvertFrom)

    err := producer.Produce(ctx, event, "mystream")
    if err != nil {
        fmt.Printf("produce %s\n", err.Error())
    }
}
```

### Consumer

```go
package app

func ConvertTo(event map[string]interface{}) (*Event, error) {
    result := new(Event)

    data, ok := event["my_body"].(string)
    if !ok {
        return result, fmt.Errorf("error convert to Event struct, %s is not exist", valueField)
    }

    err := json.Unmarshal([]byte(data), &result)
    if err != nil {
        return result, err
    }

    return result, nil
}
```

> [!IMPORTANT]
> You need to know the key where the event body is, then you can use any transformation, even proto

#### Initialization

```go
package main

import (
    "context"
    "time"

    "github.com/khv1one/goxstreams/internal/app"

    "github.com/khv1one/goxstreams"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    consumerCtx, _ := context.WithCancel(ctx)

    consumerInit(consumerCtx).
        WithConverter(app.ConvertTo).
        Run(consumerCtx)

    <-ctx.Done()
}

func consumerInit(ctx context.Context) goxstreams.Consumer[app.Event] {
    redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    config := goxstreams.ConsumerConfig{
        Stream:       "mystream",
        Group:        "mygroup",
        ConsumerName: "consumer",
    }

    myConsumer, _ := goxstreams.NewConsumer[app.Event](ctx, redisClient, app.NewWorker[app.Event]("name"), config)

    return myConsumer
}
```

> [!IMPORTANT]
> Producer and consumer must have the same conversion method

---

## Context

> I want to transfer part of my context from producer to consumer, what should I do?

Just like with the converter, you need to implement a couple of methods to pass the required parameters

### Producer

```go
package app

func CtxTransmit(ctx context.Context) (string, map[string]string) {
    ctxKeyVal := map[string]string{
        "trace_id": ctx.Value("trace_id").(string),
        "any_info": ctx.Value("any_info").(string),
    }
    return "ctxField", ctxKeyVal
}
```

- ctxField - the field where you will need to look for information inside the redis event

#### Initialization

```go
package main

import (
    "context"
    "fmt"

    "github.com/khv1one/goxstreams"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()

    event := app.Event{
        Foo: "foo", Bar: 123,
        SubEvent: app.SubEvent{BarBar: "1234"},
    }

    producer := goxstreams.
        NewProducer[app.Event](redis.NewClient(&redis.Options{Addr: "localhost:6379"})).
        WithConverter(app.ConvertFrom).
        WithCtxTransmmiter(app.CtxTransmit)

    err := producer.Produce(ctx, event, "mystream")
    if err != nil {
        fmt.Printf("produce %s\n", err.Error())
    }
}
```

### Consumer

```go
package app

func CtxReceive(ctx context.Context, event map[string]interface{}) context.Context {
    ctxS, ok := event["ctxField"].(string)
    if !ok {
        return ctx
    }

    ctxMap := make(map[string]string)
    if err := json.Unmarshal([]byte(ctxS), &ctxMap); err != nil {
        fmt.Printf("ctx receive %s\n", err.Error())
        return ctx
    }

    traceID, okTrace := ctxMap["trace_id"]
    if okTrace {
        ctx = context.WithValue(ctx, "trace_id", traceID)
    }

    tenant, okTenant := ctxMap["any_info"]
    if okTenant {
        ctx = context.WithValue(ctx, "any_info", tenant)
    }

    return ctx
}
```

#### Initialization

```go
package main

import (
    "context"
    "time"

    "github.com/khv1one/goxstreams/internal/app"

    "github.com/khv1one/goxstreams"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    consumerCtx, _ := context.WithCancel(ctx)

    consumerInit(consumerCtx).
        WithConverter(app.ConvertTo).
        WithCtxReceiver(app.CtxReceive).
        Run(consumerCtx)

    <-ctx.Done()
}

func consumerInit(ctx context.Context) goxstreams.Consumer[app.Event] {
    redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    config := goxstreams.ConsumerConfig{
        Stream:       "mystream",
        Group:        "mygroup",
        ConsumerName: "consumer",
    }

    myConsumer, _ := goxstreams.NewConsumer[app.Event](ctx, redisClient, app.NewWorker[app.Event]("name"), config)

    return myConsumer
}
```

### Result

In the worker you will receive a ctx with the keys

```go
package app

import (
    "context"
    "fmt"

    "github.com/khv1one/goxstreams"
)

func (w Worker[E]) Process(ctx context.Context, event goxstreams.RedisMessage[E]) error {
    fmt.Printf("success, retries %d, trace_id: %s, tenant: %s\n",
        event.RetryCount, ctx.Value("trace_id"), ctx.Value("tenant"))

    return nil
}
````

> [!IMPORTANT]
> You must know the key and the fields where the information is located
