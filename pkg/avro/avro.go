package avro

import (
	"encoding/binary"
	"encoding/json"
)

// AvroCodec implements the Encoder/Decoder interfaces for
// avro formats
type AvroCodec struct {
	encodeSchemaID int
	schemaCache    *SchemaCache
}

func NewAvroCodec(schemaID int, cache *SchemaCache) *AvroCodec {
	return &AvroCodec{schemaID, cache}
}

// Encode returns a binary representation of an Avro-encoded message.
func (a *AvroCodec) Encode(in json.RawMessage) ([]byte, error) {
	codec, err := a.schemaCache.getCodecForSchemaID(a.encodeSchemaID)
	if err != nil {
		return nil, err
	}

	// Creates a header with an initial zero byte and
	// the schema id encoded as a big endian uint32
	buf := make([]byte, 5)
	binary.BigEndian.PutUint32(buf[1:5], uint32(a.encodeSchemaID))

	// Convert textual json data to native Go form
	native, _, err := codec.NativeFromTextual(in)
	if err != nil {
		return nil, err
	}

	// Convert native Go form to binary Avro data
	message, err := codec.BinaryFromNative(buf, native)
	if err != nil {
		return nil, err
	}

	return message, nil
}

// Decode returns a text representation of an Avro-encoded message.
func (a *AvroCodec) Decode(in []byte) (json.RawMessage, error) {
	// Ensure avro header is present with the magic start-byte.
	if len(in) < 5 || in[0] != 0x00 {
		// The message does not contain Avro-encoded data
		return in, nil
	}

	// Schema ID is stored in the 4 bytes following the magic byte.
	schemaID := binary.BigEndian.Uint32(in[1:5])
	codec, err := a.schemaCache.getCodecForSchemaID(int(schemaID))
	if err != nil {
		return in, err
	}

	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(in[5:])
	if err != nil {
		return in, err
	}

	// Convert native Go form to textual Avro data
	message, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return in, err
	}

	return message, nil
}
