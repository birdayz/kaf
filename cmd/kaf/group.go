package main

// This file is now a shim - actual group command implementation is in pkg/commands/group.go
// The commands are registered dynamically in the PersistentPreRun hook in kaf.go

const (
	tabwriterMinWidth       = 6
	tabwriterMinWidthNested = 2
	tabwriterWidth          = 4
	tabwriterPadding        = 3
	tabwriterPadChar        = ' '
	tabwriterFlags          = 0
)
