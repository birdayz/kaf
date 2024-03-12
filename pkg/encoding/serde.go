package encoding

// Encoder reads a user-provided text string, and turns it into the wire formt.
type Encoder interface {
	Encode([]byte) ([]byte, error)
}

// Decoder reads the wire-format, and turns it into a human readable text format.
type Decoder interface {
	Decode([]byte) ([]byte, error)
}
