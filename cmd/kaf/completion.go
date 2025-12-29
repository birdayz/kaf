package main

import (
	"github.com/birdayz/kaf/pkg/commands"
)

// Completion command is special - it's registered directly in init() since it needs rootCmd
func init() {
	completionCmd := commands.GetCompletionCmd(rootCmd)
	rootCmd.AddCommand(completionCmd)
}
