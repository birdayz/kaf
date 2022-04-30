package codec

import "encoding/json"

// Encoder converts from json representation
// to bytes in the specified format
type Encoder interface {
	// Encode json to binary format
	Encode(in json.RawMessage) ([]byte, error)
}

// Decoder converts from binary representation to json
type Decoder interface {
	// Decode binary to json form
	Decode(in []byte) (json.RawMessage, error)
}

// BypassCodec is a no-op implementation of Encoder and Decoder
type BypassCodec struct{}

func (BypassCodec) Encode(in []byte) ([]byte, error) {
	return in, nil
}

func (BypassCodec) Decode(in []byte) ([]byte, error) {
	return in, nil
}
