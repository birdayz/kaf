package main

import (
	"bytes"
	"testing"

	"github.com/IBM/sarama"
)

func TestCheckHeaders(t *testing.T) {
	tests := []struct {
		name         string
		headerFilter map[string]string
		headers      []*sarama.RecordHeader
		want         bool
	}{
		{
			name:         "no filter",
			headerFilter: map[string]string{},
			headers: []*sarama.RecordHeader{
				{Key: []byte("a"), Value: []byte("b")},
			},
			want: true,
		},
		{
			name: "matching header",
			headerFilter: map[string]string{
				"a": "b",
			},
			headers: []*sarama.RecordHeader{
				{Key: []byte("a"), Value: []byte("b")},
			},
			want: true,
		},
		{
			name: "non-matching header value",
			headerFilter: map[string]string{
				"a": "c",
			},
			headers: []*sarama.RecordHeader{
				{Key: []byte("a"), Value: []byte("b")},
			},
			want: false,
		},
		{
			name: "non-matching header key",
			headerFilter: map[string]string{
				"c": "b",
			},
			headers: []*sarama.RecordHeader{
				{Key: []byte("a"), Value: []byte("b")},
			},
			want: false,
		},
		{
			name: "multiple filters match",
			headerFilter: map[string]string{
				"a": "b",
				"c": "d",
			},
			headers: []*sarama.RecordHeader{
				{Key: []byte("a"), Value: []byte("b")},
				{Key: []byte("c"), Value: []byte("d")},
			},
			want: true,
		},
		{
			name: "multiple filters one mismatch",
			headerFilter: map[string]string{
				"a": "b",
				"c": "e",
			},
			headers: []*sarama.RecordHeader{
				{Key: []byte("a"), Value: []byte("b")},
				{Key: []byte("c"), Value: []byte("d")},
			},
			want: false,
		},
		{
			name:         "no headers with filter",
			headerFilter: map[string]string{"a": "b"},
			headers:      []*sarama.RecordHeader{},
			want:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkHeaders(tt.headers, tt.headerFilter); got != tt.want {
				t.Errorf("checkHeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatMessage(t *testing.T) {
	tests := []struct {
		outputFormat OutputFormat
		name         string
		kafkaMsg     sarama.ConsumerMessage
		rawMessage   []byte
		keyToDisplay []byte
		stderr       bytes.Buffer
		want         []byte
	}{
		{
			outputFormat: OutputFormatHex,
			name:         "hex format string",
			kafkaMsg:     sarama.ConsumerMessage{Topic: "test"},
			rawMessage:   []byte("hello world"),
			keyToDisplay: []byte("key1"),
			want:         []byte("68656c6c6f20776f726c64"),
		},
	}

	for _, tt := range tests {
		// NOTE: this test mutates global state (outputFormat), must not run in parallel.
		t.Run(tt.name, func(t *testing.T) {
			outputFormat = tt.outputFormat
			if got := formatMessage(&tt.kafkaMsg, tt.rawMessage, tt.keyToDisplay, &tt.stderr); !bytes.Equal(got, tt.want) {
				t.Errorf("formatMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}
