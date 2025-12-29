package main

import (
	"bytes"

	"github.com/birdayz/kaf/pkg/proto"
	"github.com/golang/protobuf/jsonpb"
)

// protoDecode converts proto bytes to JSON using the descriptor registry
func protoDecode(reg *proto.DescriptorRegistry, b []byte, _type string) ([]byte, error) {
	dynamicMessage := reg.MessageForType(_type)
	if dynamicMessage == nil {
		return b, nil
	}

	err := dynamicMessage.Unmarshal(b)
	if err != nil {
		return nil, err
	}

	var m jsonpb.Marshaler
	var w bytes.Buffer

	err = m.Marshal(&w, dynamicMessage)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}
