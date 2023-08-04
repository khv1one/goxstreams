package goxstreams

type XRawMessage struct {
	ID         string
	RetryCount int64
	Values     map[string]interface{}
}

func NewXMessage(id string, retryCount int64, body map[string]interface{}) XRawMessage {
	return XRawMessage{id, retryCount, body}
}
