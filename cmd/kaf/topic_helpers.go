package main

import (
	"context"
)

func getKgoHighWatermarks(topic string, partitions []int32) map[int32]int64 {
	admin := getClusterAdmin()
	ctx := context.Background()

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