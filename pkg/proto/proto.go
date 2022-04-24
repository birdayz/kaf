package proto

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
)

type DescriptorRegistry struct {
	descriptors []*desc.FileDescriptor
}

func NewDescriptorRegistry(importPaths []string, exclusions []string) (*DescriptorRegistry, error) {
	p := &protoparse.Parser{
		ImportPaths: importPaths,
	}

	var protoFiles []string

	for _, importPath := range importPaths {
		err := filepath.Walk(importPath, func(path string, info os.FileInfo, err error) error {
			if info != nil && !info.IsDir() && strings.HasSuffix(path, ".proto") {
				protoFiles = append(protoFiles, path)
			}

			return nil
		})
		if err != nil {
			return nil, err
		}

	}

	resolved, err := protoparse.ResolveFilenames(importPaths, protoFiles...)
	if err != nil {
		return nil, err
	}

	var deduped []string
	for _, i := range resolved {

		var exclusionFound bool
		for _, exclusion := range exclusions {
			if strings.HasPrefix(i, exclusion) {
				exclusionFound = true
				break
			}
		}

		if !exclusionFound {
			deduped = append(deduped, i)
		}
	}

	descs, err := p.ParseFiles(deduped...)
	if err != nil {
		return nil, err
	}

	return &DescriptorRegistry{descriptors: descs}, nil
}

func (d *DescriptorRegistry) MessageForType(_type string) *dynamic.Message {
	for _, descriptor := range d.descriptors {
		if messageDescriptor := descriptor.FindMessage(_type); messageDescriptor != nil {
			return dynamic.NewMessage(messageDescriptor)
		}
	}
	return nil
}

type ProtoCodec struct {
	registry  *DescriptorRegistry
	protoType string
}

func NewProtoCodec(protoType string, registry *DescriptorRegistry) *ProtoCodec {
	return &ProtoCodec{registry, protoType}
}

func (p *ProtoCodec) Encode(in []byte) ([]byte, error) {
	if dynamicMessage := p.registry.MessageForType(p.protoType); dynamicMessage != nil {
		err := dynamicMessage.UnmarshalJSON(in)
		if err != nil {
			return nil, fmt.Errorf("failed to parse input JSON as proto type %v: %v", p.protoType, err)
		}

		pb, err := proto.Marshal(dynamicMessage)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal proto: %v", err)
		}

		return pb, nil
	} else {
		return nil, fmt.Errorf("failed to load payload proto type: %v", p.protoType)
	}
}
