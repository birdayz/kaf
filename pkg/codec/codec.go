package codec

// Encoder converts from textual representation to
// bytes in the specified format
type Encoder interface {
	// Encode textual bytes to binary format
	Encode(in []byte) ([]byte, error)
}

// Decoder converts from binary representation to
// textual in the specified format
type Decoder interface {
	// Decode binary bytes to text form
	Decode(in []byte) ([]byte, error)
}

// BypassCodec is a no-op implementation of Encoder and Decoder
type BypassCodec struct{}

func (BypassCodec) Encode(in []byte) ([]byte, error) {
	return in, nil
}

func (BypassCodec) Decode(in []byte) ([]byte, error) {
	return in, nil
}
