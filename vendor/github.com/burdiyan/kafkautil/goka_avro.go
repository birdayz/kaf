package kafkautil

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/avast/retry-go"
	"github.com/lovoo/goka"
	"github.com/pkg/errors"
)

const magicByte byte = 0

type avroRecord interface {
	Serialize(io.Writer) error
	Schema() string
}

type schemaRegisterer interface {
	RegisterNewSchema(subject string, schema string) (int, error)
}

// CodecWrapper wraps Avro goka.Codec to be compatible with
// Confluent Schema registry wire format.
type CodecWrapper interface {
	WrapCodec(c goka.Codec, subject string) goka.Codec
}

type codecWrapper struct {
	rc schemaRegisterer
}

// WrapCodec implements CodecWrapper.
func (cw *codecWrapper) WrapCodec(c goka.Codec, subject string) goka.Codec {
	return &avroCodec{
		subject:     subject,
		codec:       c,
		client:      cw.rc,
		schemaCache: make(map[string]int32),
	}
}

// NewCodecWrapper creates new CodecWrapper using provided Schema Registry client.
func NewCodecWrapper(rc schemaRegisterer) CodecWrapper {
	return &codecWrapper{rc: rc}
}

type avroCodec struct {
	subject string
	codec   goka.Codec
	client  schemaRegisterer

	mu          sync.RWMutex
	schemaCache map[string]int32
}

func (c *avroCodec) getSchema(schema string) (int32, bool) {
	c.mu.RLock()
	s, ok := c.schemaCache[schema]
	c.mu.RUnlock()
	return s, ok
}

func (c *avroCodec) saveSchema(schema string, id int32) {
	c.mu.Lock()
	c.schemaCache[schema] = id
	c.mu.Unlock()
}

// Encode implements goka.Codec and encodes value to Avro
// using Confluent Schema Registry wire format. It will register
// schema in the Schema Registry and cache the registered ID.
func (c *avroCodec) Encode(value interface{}) ([]byte, error) {
	v, ok := value.(avroRecord)
	if !ok {
		return nil, errors.Errorf("%T must implement avroRecord interface", value)
	}

	// If schema is not cached, we try to register it in Schema Registry retrying if it fails.
	id, ok := c.getSchema(v.Schema())
	if !ok {
		if err := retry.Do(func() error {
			schemaID, err := c.client.RegisterNewSchema(c.subject, v.Schema())
			if err != nil {
				return err
			}
			id = int32(schemaID)
			c.saveSchema(v.Schema(), id)
			return nil
		}, retry.Attempts(5)); err != nil {
			return nil, err
		}
	}

	var b bytes.Buffer
	b.WriteByte(magicByte)

	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(id))
	b.Write(idBytes)

	// We don't invoke underlying codec to avoid allocating another buffer,
	// and also because Avro types know how to serialize themselves, and codec is
	// just to comply with the interface required by goka.
	v.Serialize(&b)

	return b.Bytes(), nil
}

// Decode extracts Avro payload from Confluent Schema Registry wire format,
// and decodes it using underlying goka.Codec for this specific type.
func (c *avroCodec) Decode(data []byte) (interface{}, error) {
	if err := validateAvro(data); err != nil {
		return nil, err
	}

	return c.codec.Decode(data[5:])
}

func validateAvro(b []byte) error {
	if len(b) == 0 {
		return errors.New("avro: payload is empty")
	}

	if b[0] != 0 {
		return errors.Errorf("avro: wrong magic byte for confluent avro encoding: %v", b[0])
	}

	// Message encoded with Confluent Avro encoding cannot be less than 5 bytes,
	// because first byte is a magic byte, and next 4 bytes is a mandatory schema ID.
	if len(b) < 5 {
		return errors.New("avro: payload is less than 5 bytes")
	}

	return nil
}
