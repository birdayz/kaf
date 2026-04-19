package proto

import (
	"os"
	"path/filepath"

	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
)

type DescriptorRegistry struct {
	descriptors []*desc.FileDescriptor
}

func NewDescriptorRegistry(importPaths []string, exclusions []string) (*DescriptorRegistry, error) {
	parser := &protoparse.Parser{
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

	exclusionSet := make(map[string]struct{}, len(exclusions))
	for _, exclusion := range exclusions {
		exclusionSet[exclusion] = struct{}{}
	}

	var deduped []string
	seen := make(map[string]struct{})
	for _, i := range resolved {
		if _, excluded := exclusionSet[i]; excluded {
			continue
		}
		if _, ok := seen[i]; !ok {
			seen[i] = struct{}{}
			deduped = append(deduped, i)
		}
	}

	descs, err := parser.ParseFiles(deduped...)
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
