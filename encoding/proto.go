package proto

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
// _ = serde.Decoder(&ProtoEncoderDecoder{})
// _ = serde.Encoder(&ProtoEncoderDecoder{})
)

// ProtoSerde is a SerDe that converts JSON to/from Protobuf.
type ProtoEncoderDecoder struct {
	resolver          linker.Resolver
	protoType         protoreflect.FullName
	messageDescriptor protoreflect.MessageDescriptor
}

func (p *ProtoEncoderDecoder) Decode(raw []byte) ([]byte, error) {
	dynamicMessage := dynamicpb.NewMessage(p.messageDescriptor)

	// Decode into in-process type.
	err := proto.UnmarshalOptions{}.Unmarshal(raw, dynamicMessage)
	if err != nil {
		return nil, err
	}

	// Marshal into text form by using protojson.
	return protojson.MarshalOptions{
		Indent:            "  ",
		UseProtoNames:     true,
		EmitDefaultValues: true,
		Resolver:          nil,
	}.Marshal(dynamicMessage)
}

func (p *ProtoEncoderDecoder) Encode(text []byte) ([]byte, error) {
	// We got the JSON string. Use protojson.Unmarshal to unmarshal it into a dynamic message.
	dynamicMessage := dynamicpb.NewMessage(p.messageDescriptor)

	err := protojson.UnmarshalOptions{
		DiscardUnknown: true,
		Resolver:       p.resolver,
	}.Unmarshal([]byte(text), dynamicMessage)
	if err != nil {
		return nil, err
	}

	// Now we got the in-process type. Marshal it as proto binary.
	result, err := proto.Marshal(dynamicMessage)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func NewProtoEncoder(importPaths []string, protoType string) (*ProtoEncoderDecoder, error) {
	fullName := protoreflect.FullName(protoType)
	if ok := fullName.IsValid(); !ok {
		return nil, fmt.Errorf("invalid protobuf type name: %s", protoType)
	}

	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: importPaths,
		}),
	}

	allRelativePaths, err := allProtoFiles(importPaths)
	if err != nil {
		return nil, err
	}

	files, err := c.Compile(context.Background(), allRelativePaths...)
	if err != nil {
		return nil, fmt.Errorf("failed to compile: %w", err)
	}

	messageType, err := files.AsResolver().FindMessageByName(fullName)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup protobuf message type: %w", err)
	}

	return &ProtoEncoderDecoder{
		resolver:          files.AsResolver(),
		protoType:         fullName,
		messageDescriptor: messageType.Descriptor(),
	}, nil
}

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

func allProtoFiles(importPaths []string) ([]string, error) {
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
	return resolved, nil
}
