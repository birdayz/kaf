package app

import (
	"encoding/json"
	"fmt"

	"github.com/hokaccha/go-prettyjson"
	"github.com/spf13/cobra"
)

// OutputFormat controls how consumed messages are printed.
type OutputFormat string

const (
	OutputFormatDefault     OutputFormat = "default"
	OutputFormatRaw         OutputFormat = "raw"
	OutputFormatJSON        OutputFormat = "json"
	OutputFormatJSONEachRow OutputFormat = "json-each-row"
	OutputFormatHex         OutputFormat = "hex"
)

func (e *OutputFormat) String() string {
	return string(*e)
}

func (e *OutputFormat) Set(v string) error {
	switch v {
	case "default", "raw", "json", "json-each-row", "hex":
		*e = OutputFormat(v)
		return nil
	default:
		return fmt.Errorf("must be one of: default, raw, json, json-each-row, hex")
	}
}

func (e *OutputFormat) Type() string {
	return "OutputFormat"
}

// CompleteOutputFormat provides shell completion for --output.
func CompleteOutputFormat(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{"default", "raw", "json", "json-each-row", "hex"}, cobra.ShellCompDirectiveNoFileComp
}

// InputFormat controls how produced messages are read.
type InputFormat string

const (
	InputFormatDefault     InputFormat = "default"
	InputFormatJSONEachRow InputFormat = "json-each-row"
	InputFormatHex         InputFormat = "hex"
)

func (e *InputFormat) String() string {
	return string(*e)
}

func (e *InputFormat) Set(v string) error {
	switch v {
	case "default", "json-each-row", "hex":
		*e = InputFormat(v)
		return nil
	default:
		return fmt.Errorf("must be one of: default, json-each-row, hex")
	}
}

func (e *InputFormat) Type() string {
	return "InputFormat"
}

// CompleteInputFormat provides shell completion for --input.
func CompleteInputFormat(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{"default", "json-each-row", "hex"}, cobra.ShellCompDirectiveNoFileComp
}

// IsJSON returns true if data is valid JSON.
func IsJSON(data []byte) bool {
	var i any
	return json.Unmarshal(data, &i) == nil
}

// FormatJSON unmarshals data to an interface for JSON re-encoding.
// Returns the string representation if not valid JSON.
func FormatJSON(data []byte) any {
	var i any
	if err := json.Unmarshal(data, &i); err != nil {
		return string(data)
	}
	return i
}

// FormatValue pretty-prints JSON data.
func FormatValue(data []byte) []byte {
	if b, err := prettyjson.Format(data); err == nil {
		return b
	}
	return data
}
