package goxstreams

import "time"

// ConsumerConfig is configuration set for consumer work
//
//	Stream: name of the stream where we read it from
//
//	Group: each group processes messages independently of the other
//
//	ConsumerName: client name in the group, may not be unique
//
//	BatchSize: (optional) the size of messages read from the stream per request
//
//	MaxConcurrency: (optional) maximum number of message processing goroutines
//
//	NoAck: (optional) when true - messages will not be reprocessed if there was an error
//
//	MaxRetries: (optional) the number of times the message will be reprocessed on errors
//
//	CleaneUp: (optional) automatic deletion of messages after successful processing
//
//	FailReadTime: (optional) Failed messages read interval
//
//	FailIdle: (optional) The time after which the message will be considered corrupted
//
//	Example:
//	ConsumerConfig{
//		Stream:         "mystream",
//		Group:          "mygroup",
//		ConsumerName:   "consumer",
//		BatchSize:      100,
//		MaxConcurrency: 50,
//		MaxRetries:     3,
//	}
type ConsumerConfig struct {
	Stream         string
	Group          string
	ConsumerName   string
	BatchSize      int64         // Default: 10
	MaxConcurrency int64         // Default: 20
	NoAck          bool          // Default: false
	MaxRetries     int64         // Default: 0
	CleaneUp       bool          // Default: false
	ReadInterval   time.Duration // Default: 1 second

	FailIdle time.Duration // Default: 1 minute
}

func (c *ConsumerConfig) setDefaults() {
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = 20
	}

	if c.BatchSize == 0 {
		size := c.MaxConcurrency / 10
		if size < 10 {
			size = 10
		}
	}

	if c.ReadInterval == 0 {
		c.ReadInterval = 1000 * time.Millisecond
	}

	if c.FailIdle == 0 {
		c.FailIdle = 60 * time.Second
	}
}
