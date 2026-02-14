package completion

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/birdayz/kaf/pkg/app"
)

// NewCommand returns the "kaf completion" command.
// It takes the root command so it can generate completions for the full tree.
func NewCommand(root *cobra.Command, a *app.App) *cobra.Command {
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
		Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				if err := root.GenBashCompletion(a.OutWriter); err != nil {
					return fmt.Errorf("failed to generate bash completion: %w", err)
				}
			case "zsh":
				if err := root.GenZshCompletion(a.OutWriter); err != nil {
					return fmt.Errorf("failed to generate zsh completion: %w", err)
				}
			case "fish":
				if err := root.GenFishCompletion(a.OutWriter, true); err != nil {
					return fmt.Errorf("failed to generate fish completion: %w", err)
				}
			case "powershell":
				if err := root.GenPowerShellCompletion(a.OutWriter); err != nil {
					return fmt.Errorf("failed to generate powershell completion: %w", err)
				}
			}
			return nil
		},
	}
}
