package main

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
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
	p := kafka.Preset(kafka.WithTopics("kaf-testing", "gnomock-kafka"))
	c, err := gnomock.Start(p)
	if err != nil {
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

	bs, err := ioutil.ReadAll(b)
	require.NoError(t, err)

	return string(bs)
}

func runCmdWithBroker(t *testing.T, in io.Reader, args ...string) string {
	args = append([]string{"-b", kafkaAddr}, args...)
	return runCmd(t, in, args...)
}
