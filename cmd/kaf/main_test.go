package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/kafka"
	"github.com/stretchr/testify/require"
)

var kafkaAddr string

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) (code int) {
	c, err := gnomock.Start(
		kafka.Preset(kafka.WithTopics("kaf-testing", "gnomock-kafka")),
		gnomock.WithContainerName("kaf-kafka"),
	)
	if err != nil {
		log.Println(err)
		return 1
	}

	defer func() {
		stopErr := gnomock.Stop(c)
		if stopErr != nil {
			code = 1
		}
	}()

	kafkaAddr = c.Address(kafka.BrokerPort)

	return m.Run()
}

func runCmd(t *testing.T, in io.Reader, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	b := bytes.NewBufferString("")

	rootCmd.SetArgs(args)
	rootCmd.SetOut(b)
	rootCmd.SetErr(b)
	rootCmd.SetIn(in)

	require.NoError(t, rootCmd.ExecuteContext(ctx))

	bs, err := io.ReadAll(b)
	require.NoError(t, err)

	return string(bs)
}

func runCmdWithBroker(t *testing.T, in io.Reader, args ...string) string {
	args = append([]string{"-b", kafkaAddr}, args...)
	return runCmd(t, in, args...)
}
