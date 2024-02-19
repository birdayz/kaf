package config

import (
	"fmt"
	"os"

	kafv1 "github.com/birdayz/kaf/proto/gen/go/kaf/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

var unmarshalOptions = protojson.UnmarshalOptions{
	DiscardUnknown: true,
	Resolver:       nil,
	RecursionLimit: 10,
}

var marshalOptions = protojson.MarshalOptions{
	UseProtoNames:     true,
	EmitDefaultValues: true,
}

func Load(path string) (*kafv1.Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read profile: %w", err)
	}

	jzon, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert profile YAML config to internal JSON representation: %w", err)
	}

	var profile kafv1.Profile
	if err := unmarshalOptions.Unmarshal(jzon, &profile); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration: %w", err)
	}

	return &profile, nil
}

func Save(profile *kafv1.Profile, path string) error {
	jzon, err := marshalOptions.Marshal(profile)
	if err != nil {
		return fmt.Errorf("failed to marshal configuration: %w", err)
	}

	data, err := yaml.JSONToYAML(jzon)
	if err != nil {
		return fmt.Errorf("failed to convert profile internal JSON representation to YAML: %w", err)
	}

	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("failed to write profile to %s: %w", path, err)
	}

	return nil
}
