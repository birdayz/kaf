package main

import (
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
