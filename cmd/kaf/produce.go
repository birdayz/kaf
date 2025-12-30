package main

import (
	"github.com/birdayz/kaf/pkg/commands"
)

// This file is now a shim - actual produce command implementation is in pkg/commands/produce.go
// The command is registered dynamically in registerCommands() in kaf.go

// Type aliases for backward compatibility
type ProduceOptions = commands.ProduceOptions
type InputFormat = commands.InputFormat

const (
	InputFormatDefault     = commands.InputFormatDefault
	InputFormatJSONEachRow = commands.InputFormatJSONEachRow
)
