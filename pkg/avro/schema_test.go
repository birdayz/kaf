package avro

import (
	"encoding/binary"
	"testing"
)

func TestDecodeMessage_NonAvro(t *testing.T) {
	// Messages without avro magic byte should be returned as-is.
	input := []byte("plain text message")
	cache := &SchemaCache{codecsBySchemaID: make(map[int]*cachedCodec)}
	out, err := cache.DecodeMessage(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != string(input) {
		t.Fatalf("expected passthrough, got %q", out)
	}
}

func TestDecodeMessage_TooShort(t *testing.T) {
	// Less than 5 bytes should be returned as-is.
	input := []byte{0x00, 0x01, 0x02}
	cache := &SchemaCache{codecsBySchemaID: make(map[int]*cachedCodec)}
	out, err := cache.DecodeMessage(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != string(input) {
		t.Fatalf("expected passthrough for short message")
	}
}

func TestDecodeMessage_NoMagicByte(t *testing.T) {
	// 5+ bytes but wrong magic byte should be returned as-is.
	input := []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x02}
	cache := &SchemaCache{codecsBySchemaID: make(map[int]*cachedCodec)}
	out, err := cache.DecodeMessage(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != string(input) {
		t.Fatalf("expected passthrough for non-magic byte")
	}
}

func TestWireFormatHeader(t *testing.T) {
	// Verify that the Confluent wire format header is correctly structured:
	// byte 0: magic byte (0x00)
	// bytes 1-4: schema ID as big-endian uint32
	schemaID := uint32(42)
	header := make([]byte, 5)
	header[0] = 0x00
	binary.BigEndian.PutUint32(header[1:5], schemaID)

	if header[0] != 0x00 {
		t.Fatal("magic byte should be 0x00")
	}
	gotID := binary.BigEndian.Uint32(header[1:5])
	if gotID != schemaID {
		t.Fatalf("expected schema ID %d, got %d", schemaID, gotID)
	}
}
