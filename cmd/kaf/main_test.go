package main

import (
	"bytes"
	"io"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func runCmd(t *testing.T, in io.Reader, args ...string) string {
	b := bytes.NewBufferString("")

	// Reset all flags to avoid contamination between test runs
	// This is needed because cobra commands are reused across multiple Execute() calls in tests
	rootCmd.Flags().VisitAll(func(flag *pflag.Flag) {
		flag.Value.Set(flag.DefValue)
		flag.Changed = false
	})
	for _, cmd := range rootCmd.Commands() {
		resetCommandFlags(cmd)
	}

	rootCmd.SetArgs(args)
	rootCmd.SetOut(b)
	rootCmd.SetErr(b)
	rootCmd.SetIn(in)

	err := rootCmd.Execute()
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

func resetCommandFlags(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		flag.Value.Set(flag.DefValue)
		flag.Changed = false
	})
	for _, subCmd := range cmd.Commands() {
		resetCommandFlags(subCmd)
	}
}

// runCmdWithBroker runs a kaf command with the specified broker address
func runCmdWithBroker(t *testing.T, kafkaAddr string, in io.Reader, args ...string) string {
	args = append([]string{"-b", kafkaAddr}, args...)
	return runCmd(t, in, args...)
}

// runCmdAllowFail runs a kaf command and allows it to fail, returning the output and error
func runCmdAllowFail(t *testing.T, in io.Reader, args ...string) (string, error) {
	b := bytes.NewBufferString("")

	// Reset all flags to avoid contamination between test runs
	rootCmd.Flags().VisitAll(func(flag *pflag.Flag) {
		flag.Value.Set(flag.DefValue)
		flag.Changed = false
	})
	for _, cmd := range rootCmd.Commands() {
		resetCommandFlags(cmd)
	}

	rootCmd.SetArgs(args)
	rootCmd.SetOut(b)
	rootCmd.SetErr(b)
	rootCmd.SetIn(in)

	err := rootCmd.Execute()
	bs, _ := io.ReadAll(b)

	return string(bs), err
}

// runCmdWithBrokerAllowFail runs a kaf command with the specified broker address and allows it to fail
func runCmdWithBrokerAllowFail(t *testing.T, kafkaAddr string, in io.Reader, args ...string) (string, error) {
	args = append([]string{"-b", kafkaAddr}, args...)
	return runCmdAllowFail(t, in, args...)
}
