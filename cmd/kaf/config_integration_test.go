package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Note: Many config commands require interactive prompts or modify system configuration.
	// We focus on safe, non-interactive commands that can be tested reliably.

	t.Run("ConfigHelp", func(t *testing.T) {
		// Test config command help - this is always safe to run
		output := runCmd(t, nil, "config", "--help")

		// Should contain help information
		assert.Contains(t, output, "Handle kaf configuration")
		assert.Contains(t, output, "Available Commands:")
		assert.Contains(t, output, "current-context")
		assert.Contains(t, output, "add-cluster")
	})

	// Skip interactive commands like current-context, ls as they may require
	// cluster selection or configuration that isn't suitable for automated testing.
}

func TestConfigCommandsReadOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("ConfigHelp", func(t *testing.T) {
		// Test config command help
		output := runCmd(t, nil, "config", "--help")

		// Should contain help information
		assert.Contains(t, output, "Handle kaf configuration")
		assert.Contains(t, output, "Available Commands:")
		assert.Contains(t, output, "current-context")
		assert.Contains(t, output, "add-cluster")
	})

	t.Run("ConfigSubcommandHelp", func(t *testing.T) {
		// Test individual subcommand help
		testCases := []struct {
			name         string
			subcommand   string
			expectedText string
		}{
			{
				name:         "CurrentContextHelp",
				subcommand:   "current-context",
				expectedText: "Displays the current context",
			},
			{
				name:         "SelectClusterHelp",
				subcommand:   "select-cluster",
				expectedText: "Interactively select a cluster",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				output := runCmd(t, nil, "config", tc.subcommand, "--help")
				assert.Contains(t, output, tc.expectedText)
			})
		}
	})
}

// Note: We avoid testing add-cluster, remove-cluster, use-cluster commands
// in integration tests as they would modify the actual kaf configuration
// and could interfere with the test environment or user's configuration.
// These commands would be better tested in unit tests with mocked file operations.
