package main

import (
	"os"

	"github.com/birdayz/kaf/pkg/cmd"
)

// Set by goreleaser / GitHub Actions via ldflags.
var (
	commit  = "HEAD"
	version = "latest"
)

func main() {
	if err := cmd.Execute(version, commit); err != nil {
		os.Exit(1)
	}
}
