package proto

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type DescriptorRegistry struct {
	files linker.Files
}

func NewDescriptorRegistry(importPaths []string, exclusions []string) (*DescriptorRegistry, error) {
	var protoFiles []string

	for _, importPath := range importPaths {
		err := filepath.WalkDir(importPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".proto") {
				return nil
			}
			rel, err := filepath.Rel(importPath, path)
			if err != nil {
				return err
			}
			for _, exclusion := range exclusions {
				if strings.HasPrefix(rel, exclusion) {
					return nil
				}
			}
			protoFiles = append(protoFiles, rel)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			ImportPaths: importPaths,
		},
	}

	files, err := compiler.Compile(context.Background(), protoFiles...)
	if err != nil {
		return nil, err
	}

	return &DescriptorRegistry{files: files}, nil
}

func (d *DescriptorRegistry) MessageForType(_type string) *dynamicpb.Message {
	for _, f := range d.files {
		desc := f.FindDescriptorByName(protoreflect.FullName(_type))
		if desc == nil {
			continue
		}
		msgDesc, ok := desc.(protoreflect.MessageDescriptor)
		if !ok {
			continue
		}
		return dynamicpb.NewMessage(msgDesc)
	}
	return nil
}
