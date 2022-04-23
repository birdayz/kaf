package util

type Encoder interface {
	Encode(in []byte) ([]byte, error)
}

type Decoder interface {
	Decode(in []byte) ([]byte, error)
}

type BypassCodec struct{}

func (BypassCodec) Encode(in []byte) ([]byte, error) {
	return in, nil
}

func (BypassCodec) Decode(in []byte) ([]byte, error) {
	return in, nil
}
