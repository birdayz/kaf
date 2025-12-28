package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()

	t.Run("NodeList", func(t *testing.T) {
		// List nodes/brokers
		output := runCmdWithBroker(t, kafkaAddr, nil, "nodes", "ls")
		
		// Should contain broker information
		assert.NotEmpty(t, output)
		
		// Should contain headers and at least one broker
		if !strings.Contains(output, "no-headers") {
			assert.Contains(t, output, "ID")
			assert.Contains(t, output, "ADDRESS")
		}
		
		// Should contain at least one broker ID (likely 1 for single broker)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.GreaterOrEqual(t, len(lines), 1, "Should have at least one line of output")
	})

	t.Run("NodeListNoHeaders", func(t *testing.T) {
		// List nodes without headers
		output := runCmdWithBroker(t, kafkaAddr, nil, "nodes", "ls", "--no-headers")
		
		// Should not contain headers
		assert.NotContains(t, output, "ID")
		assert.NotContains(t, output, "ADDRESS")
		
		// But should still contain broker information
		assert.NotEmpty(t, strings.TrimSpace(output))
	})

	t.Run("NodeCommandAlias", func(t *testing.T) {
		// Test that 'node ls' works the same as 'nodes ls'
		output1 := runCmdWithBroker(t, kafkaAddr, nil, "node", "ls")
		output2 := runCmdWithBroker(t, kafkaAddr, nil, "nodes", "ls")
		
		// Should produce the same output
		assert.Equal(t, output1, output2)
	})
}