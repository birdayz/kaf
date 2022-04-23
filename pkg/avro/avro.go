package avro

type AvroCodec struct {
	schemaID    int
	schemaCache *SchemaCache
}

func NewAvroCodec(schemaID int, cache *SchemaCache) *AvroCodec {
  return &AvroCodec {schemaID, cache}
}

func (a *AvroCodec) Encode(in []byte) ([]byte, error) {
	return a.schemaCache.EncodeMessage(a.schemaID, in)
}

func (a *AvroCodec) Decode(in []byte) ([]byte, error) {
  return a.schemaCache.DecodeMessage(in)
}
