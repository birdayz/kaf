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
