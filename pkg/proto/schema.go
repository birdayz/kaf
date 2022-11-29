package proto

import (
	"encoding/binary"
	"fmt"

	"github.com/riferrei/srclient"
)

type SchemaClient struct {
	client srclient.ISchemaRegistryClient
}

func NewSchemaClient(url string, key string, secret string) *SchemaClient {
	schemaClient := srclient.CreateSchemaRegistryClient(url)
	schemaClient.SetCredentials(key, secret)
	return &SchemaClient{client: schemaClient}
}

// HeaderForTopic returns a Confluent Wire Format header that must be pre-pended to messages
// for Confluent to be able to decode messages using Schema Registry
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
// This is made available while we are in the midst of transitioning
// to having the Confluent header on all of our messages Once all
// messages use the header, it should be simpler to just use the
// producer interceptor (NewConfluentProducerInterceptor) above rather
// than doing this every time we produce to kafka
func (sc SchemaClient) HeaderForTopic(topic string) ([]byte, error) {
	// Default Confluent naming strategy appends -value to schemas for message body
	// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#subject-name-strategy
	schema, err := sc.client.GetLatestSchema(fmt.Sprintf("%s-value", topic))
	if err != nil {
		return nil, fmt.Errorf("unable to get topic schema %s: %w", topic, err)
	}
	if schema == nil {
		return nil, fmt.Errorf("nil schema for topic %s", topic)
	}
	return confluentHeader(schema.ID()), nil
}

// confluent header format: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func confluentHeader(schemaID int) []byte {
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))

	var header []byte
	// Magic Byte, always 0
	header = append(header, byte(0))
	// Schema ID
	header = append(header, schemaIDBytes...)
	// Protobuf message index. We assume 0 and only upload one message type for each topic
	header = append(header, byte(0))
	return header
}
