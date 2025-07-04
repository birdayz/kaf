package main

import "time"

type MessageHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type JSONEachRowMessage struct {
	Topic     string          `json:"topic"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Timestamp time.Time       `json:"timestamp"`
	Headers   []MessageHeader `json:"headers"`
	Key       string          `json:"key"`
	Payload   string          `json:"payload"`
}
