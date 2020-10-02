# Gnomock Kafka

Gnomock Kafka is a [Gnomock](https://github.com/orlangure/gnomock) preset for
running tests against a real Kafka event streaming platform, without mocks.

```go
package kafka_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/kafka"
	kafkaclient "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// nolint:funlen
func TestPreset(t *testing.T) {
	t.Parallel()

	messages := []kafka.Message{
		{
			Topic: "events",
			Key:   "order",
			Value: "1",
			Time:  time.Now().UnixNano(),
		},
		{
			Topic: "alerts",
			Key:   "CPU",
			Value: "92",
			Time:  time.Now().UnixNano(),
		},
	}

	p := kafka.Preset(
		kafka.WithTopics("topic-1", "topic-2"),
		kafka.WithMessages(messages...),
	)

	container, err := gnomock.Start(
		p,
		gnomock.WithDebugMode(), gnomock.WithLogWriter(os.Stdout),
		gnomock.WithContainerName("kafka"),
	)
	require.NoError(t, err)

	defer func() { require.NoError(t, gnomock.Stop(container)) }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	alertsReader := kafkaclient.NewReader(kafkaclient.ReaderConfig{
		Brokers: []string{container.Address(kafka.BrokerPort)},
		Topic:   "alerts",
	})

	m, err := alertsReader.ReadMessage(ctx)
	require.NoError(t, err)
	require.NoError(t, alertsReader.Close())

	require.Equal(t, "CPU", string(m.Key))
	require.Equal(t, "92", string(m.Value))

	eventsReader := kafkaclient.NewReader(kafkaclient.ReaderConfig{
		Brokers: []string{container.Address(kafka.BrokerPort)},
		Topic:   "events",
	})

	m, err = eventsReader.ReadMessage(ctx)
	require.NoError(t, err)
	require.NoError(t, eventsReader.Close())

	require.Equal(t, "order", string(m.Key))
	require.Equal(t, "1", string(m.Value))

	c, err := kafkaclient.Dial("tcp", container.Address(kafka.BrokerPort))
	require.NoError(t, err)

	require.NoError(t, c.DeleteTopics("topic-1", "topic-2"))
	require.Error(t, c.DeleteTopics("unknown-topic"))

	require.NoError(t, c.Close())
}
```
