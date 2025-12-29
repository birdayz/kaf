package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

// GetCompletionCmd returns the completion command
// rootCmd is needed to generate completions for the entire CLI
func GetCompletionCmd(rootCmd *cobra.Command) *cobra.Command {
	return &cobra.Command{
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
		RunE: func(cmd *cobra.Command, args []string) error {
			// Use cmd.OutOrStdout() to respect the output set by tests or CLI
			out := cmd.OutOrStdout()

			switch args[0] {
			case "bash":
				if err := rootCmd.GenBashCompletion(out); err != nil {
					return fmt.Errorf("failed to generate bash completion: %w", err)
				}
			case "zsh":
				if err := rootCmd.GenZshCompletion(out); err != nil {
					return fmt.Errorf("failed to generate zsh completion: %w", err)
				}
			case "fish":
				if err := rootCmd.GenFishCompletion(out, true); err != nil {
					return fmt.Errorf("failed to generate fish completion: %w", err)
				}
			case "powershell":
				if err := rootCmd.GenPowerShellCompletion(out); err != nil {
					return fmt.Errorf("failed to generate powershell completion: %w", err)
				}
			}
			return nil
		},
	}
}
