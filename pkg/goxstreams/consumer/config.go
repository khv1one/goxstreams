package consumer

import "time"

type Config struct {
	Stream         string
	Group          string
	ConsumerName   string
	BatchSize      int64
	MaxConcurrency int64
	NoAck          bool
	MaxRetries     int64
	CleaneUp       bool

	FailIdle     time.Duration
	FailReadTime time.Duration
}

func (c Config) setDefaults() {
	if c.BatchSize == 0 {
		c.BatchSize = 1
	}

	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = 1
	}

	if c.FailReadTime == 0 {
		c.FailReadTime = 1000 * time.Millisecond
	}

	if c.FailIdle == 0 {
		c.FailIdle = 2000 * time.Millisecond
	}
}
