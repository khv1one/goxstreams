package goxstreams

// RedisMessage transmite to Worker.Process and Worker.ProcessDead method. Contains eventbody and addititional info.
type RedisMessage[E any] struct {
	ID         string
	RetryCount int
	Body       E
}

func newRedisMessage[E any](id string, retryCount int64, message E) RedisMessage[E] {
	return RedisMessage[E]{id, int(retryCount), message}
}

// RedisBrokenMessage transmite to Worker.ProcessBroken method. Contains eventbody and addititional info.
type RedisBrokenMessage struct {
	ID         string
	RetryCount int
	Body       map[string]interface{}
	Error      error
}

func newRedisBrokenMessage(id string, retryCount int64, message map[string]interface{}, err error) RedisBrokenMessage {
	return RedisBrokenMessage{id, int(retryCount), message, err}
}

type xRawMessage struct {
	ID         string
	RetryCount int64
	Values     map[string]interface{}
}

func newXMessage(id string, retryCount int64, body map[string]interface{}) xRawMessage {
	return xRawMessage{id, retryCount, body}
}
