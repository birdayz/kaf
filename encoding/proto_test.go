package proto

import (
	"testing"

	testv1 "github.com/birdayz/kaf/proto/gen/go"
	"github.com/stretchr/testify/require"
	googleProto "google.golang.org/protobuf/proto"
)

func TestEncode(t *testing.T) {
	e, err := NewProtoEncoder([]string{"./testdata"}, "test.v1.Test")
	require.NoError(t, err)
	wireFormat, err := e.Encode([]byte(`{"some_field":"y"}`))
	require.NoError(t, err)

	// Decode via generated proto
	var msg testv1.Test
	err = googleProto.Unmarshal(wireFormat, &msg)
	require.NoError(t, err)
	require.Equal(t, "y", msg.SomeField)

	// Decode dynamically via encoder
	text, err := e.Decode(wireFormat)
	require.NoError(t, err)
	require.JSONEq(t, `{"some_field":"y"}`, string(text))
}

func TestEncodeTypeNotFound(t *testing.T) {
	_, err := NewProtoEncoder([]string{"./testdata"}, "test.v1.DoesNotExist")
	require.ErrorContains(t, err, "failed to lookup protobuf message type")
}
