package avro

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

// registerSchema registers an Avro schema under the given subject and returns
// the schema ID assigned by the registry.
func registerSchema(t *testing.T, srURL, subject, schema string) int {
	t.Helper()

	body := fmt.Sprintf(`{"schema": %s}`, strconv.Quote(schema))
	resp, err := http.Post(
		srURL+"/subjects/"+subject+"/versions",
		"application/vnd.schemaregistry.v1+json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "schema registration failed")

	var result struct {
		ID int `json:"id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.NotZero(t, result.ID, "schema ID should not be zero")
	return result.ID
}

// startRedpanda creates a fresh Redpanda container and returns the schema
// registry URL. The container is terminated when the test completes.
func startRedpanda(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	srURL, err := rpContainer.SchemaRegistryAddress(ctx)
	require.NoError(t, err)
	return srURL
}

const simpleSchema = `{"type":"record","name":"Test","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

func TestEncodeDecodeRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	srURL := startRedpanda(t)

	schemaID := registerSchema(t, srURL, "roundtrip-value", simpleSchema)

	cache, err := NewSchemaCache(srURL, "", "")
	require.NoError(t, err)

	input := []byte(`{"name":"Alice","age":30}`)
	encoded, err := cache.EncodeMessage(schemaID, input)
	require.NoError(t, err)
	require.True(t, len(encoded) >= 5, "encoded message must have at least 5-byte header")
	assert.Equal(t, byte(0x00), encoded[0], "first byte must be the magic byte")

	decoded, err := cache.DecodeMessage(encoded)
	require.NoError(t, err)

	// Compare as parsed JSON to ignore field ordering differences.
	var expected, actual map[string]any
	require.NoError(t, json.Unmarshal(input, &expected))
	require.NoError(t, json.Unmarshal(decoded, &actual))
	assert.Equal(t, expected, actual)
}

func TestDecodeNonAvroMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	srURL := startRedpanda(t)

	cache, err := NewSchemaCache(srURL, "", "")
	require.NoError(t, err)

	input := []byte("this is not avro data at all")
	output, err := cache.DecodeMessage(input)
	require.NoError(t, err)
	assert.Equal(t, input, output, "non-Avro message should pass through unchanged")
}

func TestDecodeShortMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	srURL := startRedpanda(t)

	cache, err := NewSchemaCache(srURL, "", "")
	require.NoError(t, err)

	inputs := [][]byte{
		{},
		{0x00},
		{0x00, 0x01},
		{0x00, 0x01, 0x02},
		{0x00, 0x01, 0x02, 0x03},
	}

	for _, input := range inputs {
		output, err := cache.DecodeMessage(input)
		require.NoError(t, err)
		assert.Equal(t, input, output, "message shorter than 5 bytes should pass through")
	}
}

func TestDecodeMissingMagicByte(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	srURL := startRedpanda(t)

	cache, err := NewSchemaCache(srURL, "", "")
	require.NoError(t, err)

	// 6 bytes, but first byte is not the magic byte 0x00.
	input := []byte{0xFF, 0x00, 0x00, 0x00, 0x01, 0x42}
	output, err := cache.DecodeMessage(input)
	require.NoError(t, err)
	assert.Equal(t, input, output, "message without magic byte should pass through")
}

func TestDecodeInvalidSchemaID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	srURL := startRedpanda(t)

	cache, err := NewSchemaCache(srURL, "", "")
	require.NoError(t, err)

	// Construct a message with the magic byte but a schema ID that does not
	// exist in the registry.
	msg := make([]byte, 10)
	msg[0] = 0x00
	binary.BigEndian.PutUint32(msg[1:5], 99999)
	// Some garbage payload bytes after the header.
	copy(msg[5:], []byte{0x01, 0x02, 0x03, 0x04, 0x05})

	_, err = cache.DecodeMessage(msg)
	require.Error(t, err, "decoding with non-existent schema ID should fail")
}

func TestEncodeInvalidSchemaID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	srURL := startRedpanda(t)

	cache, err := NewSchemaCache(srURL, "", "")
	require.NoError(t, err)

	_, err = cache.EncodeMessage(99999, []byte(`{"name":"Bob","age":25}`))
	require.Error(t, err, "encoding with non-existent schema ID should fail")
}

func TestEncodeInvalidJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	srURL := startRedpanda(t)

	schemaID := registerSchema(t, srURL, "invalid-json-value", simpleSchema)

	cache, err := NewSchemaCache(srURL, "", "")
	require.NoError(t, err)

	_, err = cache.EncodeMessage(schemaID, []byte(`not valid json`))
	require.Error(t, err, "encoding invalid JSON should fail")
}

func TestSchemaCache(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	srURL := startRedpanda(t)

	schemaID := registerSchema(t, srURL, "cache-test-value", simpleSchema)

	cache, err := NewSchemaCache(srURL, "", "")
	require.NoError(t, err)

	msg1 := []byte(`{"name":"CacheTest1","age":10}`)
	msg2 := []byte(`{"name":"CacheTest2","age":20}`)

	encoded1, err := cache.EncodeMessage(schemaID, msg1)
	require.NoError(t, err)

	encoded2, err := cache.EncodeMessage(schemaID, msg2)
	require.NoError(t, err)

	decoded1, err := cache.DecodeMessage(encoded1)
	require.NoError(t, err)

	decoded2, err := cache.DecodeMessage(encoded2)
	require.NoError(t, err)

	var out1, out2 map[string]any
	require.NoError(t, json.Unmarshal(decoded1, &out1))
	require.NoError(t, json.Unmarshal(decoded2, &out2))

	assert.Equal(t, "CacheTest1", out1["name"])
	assert.Equal(t, float64(10), out1["age"])
	assert.Equal(t, "CacheTest2", out2["name"])
	assert.Equal(t, float64(20), out2["age"])
}

func TestComplexSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	srURL := startRedpanda(t)

	complexSchema := `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "username", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null},
			{"name": "active", "type": "boolean"},
			{"name": "address", "type": {
				"type": "record",
				"name": "Address",
				"fields": [
					{"name": "street", "type": "string"},
					{"name": "city", "type": "string"},
					{"name": "zip", "type": "string"}
				]
			}},
			{"name": "tags", "type": {"type": "array", "items": "string"}}
		]
	}`

	schemaID := registerSchema(t, srURL, "complex-value", complexSchema)

	cache, err := NewSchemaCache(srURL, "", "")
	require.NoError(t, err)

	input := []byte(`{
		"id": 42,
		"username": "torvalds",
		"email": {"string": "linus@kernel.org"},
		"active": true,
		"address": {
			"street": "123 Kernel Lane",
			"city": "Portland",
			"zip": "97201"
		},
		"tags": ["linux", "git"]
	}`)

	encoded, err := cache.EncodeMessage(schemaID, input)
	require.NoError(t, err)

	decoded, err := cache.DecodeMessage(encoded)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(decoded, &result))

	assert.Equal(t, float64(42), result["id"])
	assert.Equal(t, "torvalds", result["username"])
	assert.Equal(t, true, result["active"])

	addr, ok := result["address"].(map[string]any)
	require.True(t, ok, "address should be a map")
	assert.Equal(t, "123 Kernel Lane", addr["street"])
	assert.Equal(t, "Portland", addr["city"])
	assert.Equal(t, "97201", addr["zip"])

	tags, ok := result["tags"].([]any)
	require.True(t, ok, "tags should be an array")
	assert.Equal(t, []any{"linux", "git"}, tags)
}
