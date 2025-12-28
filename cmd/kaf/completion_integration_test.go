package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompletionCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("CompletionBash", func(t *testing.T) {
		// Generate bash completion
		output := runCmd(t, nil, "completion", "bash")
		
		// Should contain bash completion script
		assert.Contains(t, output, "bash completion")
		assert.Contains(t, output, "__kaf_")
		// Should contain function definitions
		assert.Contains(t, output, "function")
	})

	t.Run("CompletionZsh", func(t *testing.T) {
		// Generate zsh completion
		output := runCmd(t, nil, "completion", "zsh")
		
		// Should contain zsh completion script
		assert.Contains(t, output, "#compdef")
		assert.NotEmpty(t, strings.TrimSpace(output))
	})

	t.Run("CompletionFish", func(t *testing.T) {
		// Generate fish completion
		output := runCmd(t, nil, "completion", "fish")
		
		// Should contain fish completion script
		assert.Contains(t, output, "complete")
		assert.NotEmpty(t, strings.TrimSpace(output))
	})

	t.Run("CompletionPowershell", func(t *testing.T) {
		// Generate powershell completion
		output := runCmd(t, nil, "completion", "powershell")
		
		// Should contain powershell completion script
		assert.NotEmpty(t, strings.TrimSpace(output))
		// PowerShell completions typically contain Register-ArgumentCompleter
		assert.Contains(t, output, "Register-ArgumentCompleter")
	})

	t.Run("CompletionHelp", func(t *testing.T) {
		// Test completion command help
		output := runCmd(t, nil, "completion", "--help")
		
		// Should contain help information
		assert.Contains(t, output, "To load completions")
		assert.Contains(t, output, "bash")
		assert.Contains(t, output, "zsh")
		assert.Contains(t, output, "fish")
		assert.Contains(t, output, "Usage:")
	})

	// Error cases
	t.Run("CompletionInvalidShell", func(t *testing.T) {
		// Try to generate completion for invalid shell
		output := runCmd(t, nil, "completion", "invalid-shell")
		
		// Should contain help or error message (command may show help on invalid input)
		assert.NotEmpty(t, output)
		// May contain usage information rather than specific error
		assert.Contains(t, output, "Usage:")
	})

	t.Run("CompletionNoArgs", func(t *testing.T) {
		// Try completion command without shell argument
		output := runCmd(t, nil, "completion")
		
		// Should show usage or error
		assert.NotEmpty(t, output)
		// Should mention required shell argument
		if !strings.Contains(output, "Usage:") {
			assert.Contains(t, output, "required")
		}
	})
}