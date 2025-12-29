package main

import (
	"context"

	"github.com/birdayz/kaf/pkg/commands"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This file is now a shim - actual consume command implementation is in pkg/commands/consume.go
// The command is registered dynamically in registerCommands() in kaf.go

// Type aliases for backward compatibility with tests
type ConsumeOptions = commands.ConsumeOptions
type OutputFormat = commands.OutputFormat

const (
	OutputFormatDefault     = commands.OutputFormatDefault
	OutputFormatRaw         = commands.OutputFormatRaw
	OutputFormatJSON        = commands.OutputFormatJSON
	OutputFormatJSONEachRow = commands.OutputFormatJSONEachRow
)

// ConsumeMessages is a wrapper for backward compatibility with tests
func ConsumeMessages(ctx context.Context, opts ConsumeOptions) error {
	// Create a minimal Commands instance for testing
	// The integration tests will set currentCluster before calling this
	cmds := getOrCreateCommands()
	return cmds.ConsumeMessages(ctx, opts, make(map[string]string))
}

// checkHeaders is a wrapper for backward compatibility with tests
func checkHeaders(headers []kgo.RecordHeader, filter map[string]string) bool {
	return commands.CheckHeaders(headers, filter)
}
