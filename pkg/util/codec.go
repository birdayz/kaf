package util

type Encoder interface {
	Encode(in []byte) ([]byte, error)
}

type Decoder interface {
	Decode(in []byte) ([]byte, error)
}
