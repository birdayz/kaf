package cmd_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/birdayz/kaf/pkg/app"
	cmdconsume "github.com/birdayz/kaf/pkg/cmd/consume"
	cmdgroup "github.com/birdayz/kaf/pkg/cmd/group"
	cmdnode "github.com/birdayz/kaf/pkg/cmd/node"
	cmdproduce "github.com/birdayz/kaf/pkg/cmd/produce"
	cmdquery "github.com/birdayz/kaf/pkg/cmd/query"
	cmdtopic "github.com/birdayz/kaf/pkg/cmd/topic"
	"github.com/birdayz/kaf/pkg/config"
)

// startRedpanda starts a Redpanda container and returns the broker address.
func startRedpanda(t *testing.T, ctx context.Context) string {
	t.Helper()
	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })
	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return brokers
}

// testApp creates a fresh App wired to the given broker with captured I/O.
func testApp(t *testing.T, brokers string) (*app.App, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	a := &app.App{
		OutWriter:    out,
		ErrWriter:    errOut,
		InReader:     strings.NewReader(""),
		ColorableOut: out,
		CurrentCluster: &config.Cluster{
			Brokers: []string{brokers},
		},
	}
	return a, out, errOut
}

// runCmd executes a cobra command with args against a test context.
func runCmd(ctx context.Context, cmd *cobra.Command, args ...string) error {
	cmd.SetArgs(args)
	return cmd.ExecuteContext(ctx)
}

// --- Topic commands ---

func TestTopicCRUD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokers := startRedpanda(t, ctx)
	topicName := "test-topic-crud"

	// Create
	a, out, _ := testApp(t, brokers)
	err := runCmd(ctx, cmdtopic.NewCommand(a), "create", topicName, "-p", "3", "-r", "1")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Created topic")
	assert.Contains(t, out.String(), topicName)

	// List
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdtopic.NewCommand(a), "ls")
	require.NoError(t, err)
	assert.Contains(t, out.String(), topicName)
	assert.Contains(t, out.String(), "3") // partitions

	// Topics alias
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdtopic.NewTopicsAlias(a))
	require.NoError(t, err)
	assert.Contains(t, out.String(), topicName)

	// Describe
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdtopic.NewCommand(a), "describe", topicName)
	require.NoError(t, err)
	output := out.String()
	assert.Contains(t, output, topicName)
	assert.Contains(t, output, "Partition")

	// Add config
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdtopic.NewCommand(a), "add-config", topicName, "retention.ms", "86400000")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Added config")

	// Describe again to verify config shows
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdtopic.NewCommand(a), "describe", topicName)
	require.NoError(t, err)
	assert.Contains(t, out.String(), "retention.ms")

	// Remove config
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdtopic.NewCommand(a), "rm-config", topicName, "retention.ms")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Removed attributes")

	// Delete
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdtopic.NewCommand(a), "delete", topicName, "--noconfirm")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Deleted topic")

	// Verify gone
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdtopic.NewCommand(a), "ls")
	require.NoError(t, err)
	assert.NotContains(t, out.String(), topicName)
}

// --- Node commands ---

func TestNodeList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokers := startRedpanda(t, ctx)

	// node ls
	a, out, _ := testApp(t, brokers)
	err := runCmd(ctx, cmdnode.NewCommand(a), "ls")
	require.NoError(t, err)
	output := out.String()
	assert.Contains(t, output, "ID")
	assert.Contains(t, output, "ADDRESS")
	assert.Contains(t, output, "true") // controller

	// nodes alias
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdnode.NewNodesAlias(a))
	require.NoError(t, err)
	assert.Contains(t, out.String(), "true")

	// nodes --no-headers
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdnode.NewNodesAlias(a), "--no-headers")
	require.NoError(t, err)
	assert.NotContains(t, out.String(), "ID")
	assert.NotContains(t, out.String(), "ADDRESS")
}

// --- Produce + Consume ---

func TestProduceConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	brokers := startRedpanda(t, ctx)
	t.Logf("Broker address: %s", brokers)
	topicName := "test-produce-consume"

	// Create topic
	a, _, _ := testApp(t, brokers)
	require.NoError(t, runCmd(ctx, cmdtopic.NewCommand(a), "create", topicName, "-p", "1", "-r", "1"))

	// Produce a message
	a, out, _ := testApp(t, brokers)
	a.InReader = strings.NewReader("hello world")
	err := runCmd(ctx, cmdproduce.NewCommand(a), topicName, "-k", "mykey")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Sent record to partition 0")

	// Produce with header
	a, out, _ = testApp(t, brokers)
	a.InReader = strings.NewReader("with header")
	err = runCmd(ctx, cmdproduce.NewCommand(a), topicName, "-k", "k2", "-H", "env:prod")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Sent record")

	// Consume (non-follow, should stop at end)
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdconsume.NewCommand(a), topicName)
	require.NoError(t, err)
	output := out.String()
	assert.Contains(t, output, "hello world")
	assert.Contains(t, output, "with header")

	// Consume with --output raw
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdconsume.NewCommand(a), topicName, "--output", "raw")
	require.NoError(t, err)
	output = out.String()
	assert.Contains(t, output, "hello world")
	// Raw mode should not contain metadata like "Partition:"
	assert.NotContains(t, output, "Partition:")

	// Consume with --limit-messages
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdconsume.NewCommand(a), topicName, "--output", "raw", "-l", "1")
	require.NoError(t, err)
	// Should have exactly one line (one message + one newline)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	assert.Equal(t, 1, len(lines), "expected exactly 1 message, got: %v", lines)

	// Consume with --output json-each-row
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdconsume.NewCommand(a), topicName, "--output", "json-each-row", "-l", "1")
	require.NoError(t, err)
	assert.Contains(t, out.String(), `"topic"`)
	assert.Contains(t, out.String(), `"payload"`)

	// Consume with --tail
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdconsume.NewCommand(a), topicName, "--output", "raw", "-n", "1")
	require.NoError(t, err)
	lines = strings.Split(strings.TrimSpace(out.String()), "\n")
	assert.Equal(t, 1, len(lines), "tail 1 should return 1 message, got: %v", lines)

	// Consume with header filter
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdconsume.NewCommand(a), topicName, "--output", "raw", "--header", "env:prod")
	require.NoError(t, err)
	output = out.String()
	assert.Contains(t, output, "with header")
	assert.NotContains(t, output, "hello world")
}

// --- Group commands ---

func TestGroupLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokers := startRedpanda(t, ctx)
	topicName := "test-group-lifecycle"
	groupName := "test-group"

	// Create topic and produce data
	a, _, _ := testApp(t, brokers)
	require.NoError(t, runCmd(ctx, cmdtopic.NewCommand(a), "create", topicName, "-p", "1", "-r", "1"))

	a, _, _ = testApp(t, brokers)
	a.InReader = strings.NewReader("msg1")
	require.NoError(t, runCmd(ctx, cmdproduce.NewCommand(a), topicName))
	a, _, _ = testApp(t, brokers)
	a.InReader = strings.NewReader("msg2")
	require.NoError(t, runCmd(ctx, cmdproduce.NewCommand(a), topicName))

	// Consume with group + commit
	a, out, _ := testApp(t, brokers)
	err := runCmd(ctx, cmdconsume.NewCommand(a), topicName, "-g", groupName, "--commit", "-l", "2")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "msg1")
	assert.Contains(t, out.String(), "msg2")

	// Wait a moment for the group to be registered
	time.Sleep(500 * time.Millisecond)

	// Group list
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdgroup.NewCommand(a), "ls")
	require.NoError(t, err)
	assert.Contains(t, out.String(), groupName)

	// Groups alias
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdgroup.NewGroupsAlias(a))
	require.NoError(t, err)
	assert.Contains(t, out.String(), groupName)

	// Group describe
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdgroup.NewCommand(a), "describe", groupName)
	require.NoError(t, err)
	output := out.String()
	assert.Contains(t, output, groupName)
	assert.Contains(t, output, topicName)

	// Group commit (reset to oldest)
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdgroup.NewCommand(a), "commit", groupName, "-t", topicName, "-o", "oldest", "-p", "0", "--noconfirm")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Successfully committed")

	// Group delete
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdgroup.NewCommand(a), "delete", groupName)
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Deleted consumer group")
}

// --- Query command ---

func TestQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokers := startRedpanda(t, ctx)
	topicName := "test-query"

	// Create topic and produce
	a, _, _ := testApp(t, brokers)
	require.NoError(t, runCmd(ctx, cmdtopic.NewCommand(a), "create", topicName, "-p", "1", "-r", "1"))

	a, _, _ = testApp(t, brokers)
	a.InReader = strings.NewReader("target-value")
	require.NoError(t, runCmd(ctx, cmdproduce.NewCommand(a), topicName, "-k", "findme"))

	a, _, _ = testApp(t, brokers)
	a.InReader = strings.NewReader("other-value")
	require.NoError(t, runCmd(ctx, cmdproduce.NewCommand(a), topicName, "-k", "other"))

	// Query by key
	a, out, _ := testApp(t, brokers)
	err := runCmd(ctx, cmdquery.NewCommand(a), topicName, "-k", "findme")
	require.NoError(t, err)
	output := out.String()
	assert.Contains(t, output, "findme")
	assert.Contains(t, output, "target-value")
	assert.NotContains(t, output, "other-value")

	// Query with grep
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdquery.NewCommand(a), topicName, "-k", "findme", "--grep", "target")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "target-value")
}

// --- Topic lag ---

func TestTopicLag(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokers := startRedpanda(t, ctx)
	topicName := "test-topic-lag"
	groupName := "test-lag-group"

	// Create topic, produce 5 messages
	a, _, _ := testApp(t, brokers)
	require.NoError(t, runCmd(ctx, cmdtopic.NewCommand(a), "create", topicName, "-p", "1", "-r", "1"))

	for i := 0; i < 5; i++ {
		a, _, _ = testApp(t, brokers)
		a.InReader = strings.NewReader("msg")
		require.NoError(t, runCmd(ctx, cmdproduce.NewCommand(a), topicName))
	}

	// Consume 2 with group (leave lag of 3)
	a, _, _ = testApp(t, brokers)
	err := runCmd(ctx, cmdconsume.NewCommand(a), topicName, "-g", groupName, "--commit", "-l", "2")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Check lag
	a, out, _ := testApp(t, brokers)
	err = runCmd(ctx, cmdtopic.NewCommand(a), "lag", topicName)
	require.NoError(t, err)
	output := out.String()
	assert.Contains(t, output, groupName)
	assert.Contains(t, output, "3") // lag of 3
}

// --- Produce edge cases ---

func TestProduceRepeat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokers := startRedpanda(t, ctx)
	topicName := "test-produce-repeat"

	a, _, _ := testApp(t, brokers)
	require.NoError(t, runCmd(ctx, cmdtopic.NewCommand(a), "create", topicName, "-p", "1", "-r", "1"))

	// Produce with --repeat
	a, out, _ := testApp(t, brokers)
	a.InReader = strings.NewReader("repeated")
	err := runCmd(ctx, cmdproduce.NewCommand(a), topicName, "-n", "3")
	require.NoError(t, err)
	assert.Equal(t, 3, strings.Count(out.String(), "Sent record"))

	// Verify all 3 are consumable
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdconsume.NewCommand(a), topicName, "--output", "raw")
	require.NoError(t, err)
	assert.Equal(t, 3, strings.Count(out.String(), "repeated"))
}

// --- Produce with partition ---

func TestProduceToPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokers := startRedpanda(t, ctx)
	topicName := "test-produce-partition"

	a, _, _ := testApp(t, brokers)
	require.NoError(t, runCmd(ctx, cmdtopic.NewCommand(a), "create", topicName, "-p", "3", "-r", "1"))

	// Produce to partition 2
	a, out, _ := testApp(t, brokers)
	a.InReader = strings.NewReader("to-p2")
	err := runCmd(ctx, cmdproduce.NewCommand(a), topicName, "-p", "2")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "partition 2")

	// Consume from partition 2 only
	a, out, _ = testApp(t, brokers)
	err = runCmd(ctx, cmdconsume.NewCommand(a), topicName, "--output", "raw", "-p", "2")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "to-p2")
}

// --- Delete offsets ---

func TestGroupDeleteOffsets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokers := startRedpanda(t, ctx)
	topicName := "test-delete-offsets"
	groupName := "test-deloff-group"

	// Setup: create topic, produce, consume with group
	a, _, _ := testApp(t, brokers)
	require.NoError(t, runCmd(ctx, cmdtopic.NewCommand(a), "create", topicName, "-p", "1", "-r", "1"))

	a, _, _ = testApp(t, brokers)
	a.InReader = strings.NewReader("msg")
	require.NoError(t, runCmd(ctx, cmdproduce.NewCommand(a), topicName))

	a, _, _ = testApp(t, brokers)
	require.NoError(t, runCmd(ctx, cmdconsume.NewCommand(a), topicName, "-g", groupName, "--commit", "-l", "1"))

	time.Sleep(500 * time.Millisecond)

	// Verify group has committed offsets
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	require.NoError(t, err)
	admin := kadm.NewClient(cl)
	offsets, err := admin.FetchOffsets(ctx, groupName)
	require.NoError(t, err)
	_, ok := offsets.Lookup(topicName, 0)
	require.True(t, ok, "expected committed offset for partition 0")
	cl.Close()

	// Delete offsets
	a, out, _ := testApp(t, brokers)
	err = runCmd(ctx, cmdgroup.NewCommand(a), "delete-offsets", groupName, "-t", topicName, "--all-partitions", "--noconfirm")
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Successfully deleted offsets")

	// Verify offsets are gone
	cl, err = kgo.NewClient(kgo.SeedBrokers(brokers))
	require.NoError(t, err)
	admin = kadm.NewClient(cl)
	offsets, err = admin.FetchOffsets(ctx, groupName)
	require.NoError(t, err)
	_, ok = offsets.Lookup(topicName, 0)
	assert.False(t, ok, "expected offset to be deleted")
	cl.Close()
}
