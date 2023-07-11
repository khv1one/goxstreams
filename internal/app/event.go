package app

import (
	"errors"
	"strconv"
)

type Event struct {
	RedisID string
	Message string
	Name    string
	Foo     int
	Bar     int
}

func (e Event) GetRedisID() string {
	return e.RedisID
}

type Converter[E Event] struct{}

func (c Converter[E]) From(event Event) map[string]interface{} {
	result := make(map[string]interface{})

	result["message"] = event.Message
	result["name"] = event.Name
	result["foo"] = event.Foo
	result["bar"] = event.Bar

	return result
}

func (c Converter[E]) To(id string, event map[string]interface{}) (Event, error) {
	result := Event{}
	message, ok := event["message"].(string)
	if !ok {
		return result, errors.New("error convert to EventStruct, message is not exist")
	}

	name, ok := event["name"].(string)
	if !ok {
		return result, errors.New("error convert to EventStruct, name is not exist")
	}

	fooStr, ok := event["foo"].(string)
	if !ok {
		return result, errors.New("error convert to EventStruct, foo is not exist")
	}
	foo, err := strconv.Atoi(fooStr)
	if err != nil {
		return result, err
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
	result.Message = message
	result.Name = name
	result.Foo = foo
	result.Bar = bar

	return result, nil
}
