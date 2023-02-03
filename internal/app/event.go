package app

import "errors"

type Event struct {
	RedisID string
	Message string
	Name    string
	Foo     int
	Bar     int
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
		return result, errors.New("error convert to EventStruct, message dont exist")
	}

	name, ok := event["name"].(string)
	if !ok {
		return result, errors.New("error convert to EventStruct, name dont exist")
	}

	foo, ok := event["foo"].(int)
	if !ok {
		return result, errors.New("error convert to EventStruct, foo dont exist")
	}

	bar, ok := event["bar"].(int)
	if !ok {
		return result, errors.New("error convert to EventStruct, bar dont exist")
	}

	result.RedisID = id
	result.Message = message
	result.Name = name
	result.Foo = foo
	result.Bar = bar

	return result, nil
}
