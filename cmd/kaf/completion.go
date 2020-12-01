package main

import (
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(completionCmd)
}

var completionCmd = &cobra.Command{
	Use:       "completion [SHELL]",
	Short:     "Generate bash completion script for bash or zsh",
	Args:      cobra.ExactValidArgs(1),
	ValidArgs: []string{"bash", "zsh", "powershell"},
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			err := rootCmd.GenBashCompletion(outWriter)
			if err != nil {
				errorExit("Failed to generate bash completion: %w", err)
			}
		case "zsh":
			if err := rootCmd.GenZshCompletion(outWriter); err != nil {
				errorExit("Failed to generate zsh completion: %w", err)
			}
		case "powershell":
			err := rootCmd.GenPowerShellCompletion(outWriter)
			if err != nil {
				errorExit("Failed to generate powershell completion: %w", err)
			}
		}
	},
}
