package main

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var (
	sharedKafkaContainer testcontainers.Container
	sharedKafkaAddr      string
	kafkaSetupOnce       sync.Once
	kafkaSetupErr        error
)

// getSharedKafka returns a shared Kafka container that is created once and reused across all tests
// This prevents creating 30+ containers which causes client creation issues
func getSharedKafka(t *testing.T) string {
	t.Helper()

	kafkaSetupOnce.Do(func() {
		ctx := context.Background()

		kafkaContainer, err := kafka.RunContainer(ctx,
			testcontainers.WithImage("confluentinc/cp-kafka:7.4.0"),
			kafka.WithClusterID("test-cluster"),
		)
		if err != nil {
			kafkaSetupErr = err
			return
		}

		brokers, err := kafkaContainer.Brokers(ctx)
		if err != nil {
			kafkaSetupErr = err
			return
		}

		if len(brokers) == 0 {
			kafkaSetupErr = err
			return
		}

		sharedKafkaContainer = kafkaContainer
		sharedKafkaAddr = brokers[0]

		// Note: We don't explicitly terminate the container
		// testcontainers' ryuk will clean it up when the process exits
	})

	require.NoError(t, kafkaSetupErr, "Failed to setup shared Kafka container")
	return sharedKafkaAddr
}
