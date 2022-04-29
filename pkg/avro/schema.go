package avro

import (
	"encoding/binary"
	"sync"

	schemaregistry "github.com/Landoop/schema-registry"
	"github.com/linkedin/goavro/v2"
)

type cachedCodec struct {
	done  chan struct{}
	codec *goavro.Codec
	err   error
}

// SchemaCache connects to the Confluent schema registry and maintains
// a cached versions of Avro schemas and codecs.
type SchemaCache struct {
	client *schemaregistry.Client

	mu               sync.RWMutex
	codecsBySchemaID map[int]*cachedCodec
}

// NewSchemaCache returns a new Cache instance
func NewSchemaCache(url string) (*SchemaCache, error) {
	client, err := schemaregistry.NewClient(url)
	if err != nil {
		return nil, err
	}

	c := &SchemaCache{
		codecsBySchemaID: make(map[int]*cachedCodec),
		client:           client,
	}
	return c, nil
}

// getCodecForSchemaID returns a goavro codec for transforming data.
func (c *SchemaCache) getCodecForSchemaID(schemaID int) (codec *goavro.Codec, err error) {
	c.mu.RLock()
	cc, ok := c.codecsBySchemaID[schemaID]
	c.mu.RUnlock()
	if ok {
		<-cc.done
		return cc.codec, cc.err
	}

	// Codec is not cached, grab exclusive lock and ensure no other
	// goroutine started the process in-between.
	c.mu.Lock()
	cc, ok = c.codecsBySchemaID[schemaID]
	if ok {
		// Another goroutine began fetching schema and codec.
		c.mu.Unlock()
		<-cc.done
		return cc.codec, cc.err
	}

	// Create the cachedCodec with a promise of a future value.
	cc = &cachedCodec{done: make(chan struct{})}
	c.codecsBySchemaID[schemaID] = cc
	c.mu.Unlock()

	defer func() {
		cc.codec = codec
		cc.err = err   // Any failure is permanent on a per-schema basis.
		close(cc.done) // Promise fulfilled.
	}()

	schema, err := c.client.GetSchemaById(schemaID)
	if err != nil {
		return nil, err
	}

	codec, err = goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}

	return codec, nil
}

// DecodeMessage returns a text representation of an Avro-encoded message.
func (c *SchemaCache) DecodeMessage(b []byte) (message []byte, err error) {
	// Ensure avro header is present with the magic start-byte.
	if len(b) < 5 || b[0] != 0x00 {
		// The message does not contain Avro-encoded data
		return b, nil
	}

	// Schema ID is stored in the 4 bytes following the magic byte.
	schemaID := binary.BigEndian.Uint32(b[1:5])
	codec, err := c.getCodecForSchemaID(int(schemaID))
	if err != nil {
		return b, err
	}

	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(b[5:])
	if err != nil {
		return b, err
	}

	// Convert native Go form to textual Avro data
	message, err = codec.TextualFromNative(nil, native)
	if err != nil {
		return b, err
	}

	return message, nil
}

// EncodeMessage returns a binary representation of an Avro-encoded message.
func (c *SchemaCache) EncodeMessage(schemaID int, json []byte) (message []byte, err error) {
	codec, err := c.getCodecForSchemaID(schemaID)
	if err != nil {
		return nil, err
	}

	// Creates a header with an initial zero byte and
	// the schema id encoded as a big endian uint32
	buf := make([]byte, 5)
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))

	// Convert textual json data to native Go form
	native, _, err := codec.NativeFromTextual(json)
	if err != nil {
		return nil, err
	}

	// Convert native Go form to binary Avro data
	message, err = codec.BinaryFromNative(buf, native)
	if err != nil {
		return nil, err
	}

	return message, nil
}
