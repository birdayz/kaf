package avro

import "encoding/binary"

// AvroCodec implements the Encoder/Decoder interfaces for
// avro formats
type AvroCodec struct {
	encodeSchemaID int
	schemaCache    *SchemaCache
}

func NewAvroCodec(schemaID int, cache *SchemaCache) *AvroCodec {
	return &AvroCodec{schemaID, cache}
}

func (a *AvroCodec) Encode(in []byte) ([]byte, error) {
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

func (a *AvroCodec) Decode(in []byte) ([]byte, error) {
	return a.schemaCache.DecodeMessage(in)
}
