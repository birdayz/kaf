package main

import (
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(completionCmd)
}

var completionCmd = &cobra.Command{
	Use:   "completion [SHELL]",
	Short: "Generate completion script for bash, zsh, fish or powershell",
	Long: `To load completions:

Bash:

$ source <(kaf completion bash)

# To load completions for each session, execute once:
Linux:
  $ kaf completion bash > /etc/bash_completion.d/kaf
MacOS:
  $ kaf completion bash > /usr/local/etc/bash_completion.d/kaf

Zsh:

# To load completions for each session, execute once:
$ kaf completion zsh > "${fpath[1]}/_kaf"

# You will need to start a new shell for this setup to take effect.

Fish:

$ kaf completion fish | source

# To load completions for each session, execute once:
$ kaf completion fish > ~/.config/fish/completions/kaf.fish
`,
	DisableFlagsInUseLine: true,
	Args:                  cobra.ExactValidArgs(1),
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
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
		case "fish":
			if err := rootCmd.GenFishCompletion(outWriter, true); err != nil {
				errorExit("Failed to generate fish completion: %w", err)
			}
		case "powershell":
			err := rootCmd.GenPowerShellCompletion(outWriter)
			if err != nil {
				errorExit("Failed to generate powershell completion: %w", err)
			}
		}
	},
}
