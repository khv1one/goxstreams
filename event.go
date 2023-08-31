package goxstreams

type xRawMessage struct {
	ID         string
	RetryCount int64
	Values     map[string]interface{}
}

func newXMessage(id string, retryCount int64, body map[string]interface{}) xRawMessage {
	return xRawMessage{id, retryCount, body}
}
