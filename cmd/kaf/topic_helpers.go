package main

import (
	"context"
	"github.com/twmb/franz-go/pkg/kadm"
)

func getKgoHighWatermarks(topic string, partitions []int32) map[int32]int64 {
	cl := getClient()
	defer cl.Close()
	ctx := context.Background()
	admin := kadm.NewClient(cl)

	offsets, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		errorExit("Unable to get available offsets: %v\n", err)
	}

	watermarks := make(map[int32]int64)
	if topicOffsets, exists := offsets[topic]; exists {
		for partition, offset := range topicOffsets {
			watermarks[partition] = offset.Offset
		}
	}
	return watermarks
}