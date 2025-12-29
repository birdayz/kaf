package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var kafkaAddr string

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) (code int) {
	ctx := context.Background()

	kafkaContainer, err := kafka.RunContainer(ctx,
		testcontainers.WithImage("confluentinc/cp-kafka:7.4.0"),
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		return 1
	}

	defer func() {
		if stopErr := kafkaContainer.Terminate(ctx); stopErr != nil {
			code = 1
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		return 1
	}

	if len(brokers) == 0 {
		return 1
	}

	kafkaAddr = brokers[0]

	return m.Run()
}

func runCmd(t *testing.T, in io.Reader, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*90) // 90 second timeout to get goroutine dump before CI timeout
	defer cancel()

	b := bytes.NewBufferString("")

	rootCmd.SetArgs(args)
	rootCmd.SetOut(b)
	rootCmd.SetErr(b)
	rootCmd.SetIn(in)

	err := rootCmd.ExecuteContext(ctx)
	if err != nil {
		// Get the output to see what went wrong
		bs, _ := io.ReadAll(b)
		t.Logf("Command failed: %v\nArgs: %v\nOutput: %s", err, args, string(bs))
		t.FailNow()
	}

	bs, err := io.ReadAll(b)
	require.NoError(t, err)

	return string(bs)
}

// runCmdWithBroker runs a kaf command with the specified broker address
func runCmdWithBroker(t *testing.T, kafkaAddr string, in io.Reader, args ...string) string {
	args = append([]string{"-b", kafkaAddr}, args...)
	return runCmd(t, in, args...)
}

// runCmdAllowFail runs a kaf command and allows it to fail, returning the output and error
func runCmdAllowFail(t *testing.T, in io.Reader, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*90) // 90 second timeout to get goroutine dump before CI timeout
	defer cancel()

	b := bytes.NewBufferString("")

	rootCmd.SetArgs(args)
	rootCmd.SetOut(b)
	rootCmd.SetErr(b)
	rootCmd.SetIn(in)

	err := rootCmd.ExecuteContext(ctx)
	bs, _ := io.ReadAll(b)

	return string(bs), err
}

// runCmdWithBrokerAllowFail runs a kaf command with the specified broker address and allows it to fail
func runCmdWithBrokerAllowFail(t *testing.T, kafkaAddr string, in io.Reader, args ...string) (string, error) {
	args = append([]string{"-b", kafkaAddr}, args...)
	return runCmdAllowFail(t, in, args...)
}
