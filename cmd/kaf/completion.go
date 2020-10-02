package main

import (
	"io"
	"text/template"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(completionCmd)
}

var completionCmd = &cobra.Command{
	Use:       "completion [SHELL]",
	Short:     "Generate bash completion script for bash or zsh",
	Args:      cobra.ExactValidArgs(1),
	ValidArgs: []string{"bash", "zsh"},
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			err := rootCmd.GenBashCompletion(outWriter)
			if err != nil {
				errorExit("Failed to generate bash completion: %w", err)
			}
		case "zsh":
			if err := genZshCompletion(outWriter); err != nil {
				errorExit("Failed to generate zsh completion: %w", err)
			}
		}
	},
}

// GenZshCompletion generates a zsh completion and writes to the passed writer.
// NOTE: Native zsh autocompletion is currently broken in cobra package.
// This is workaround taken from https://github.com/spf13/cobra/pull/828
// For more info see https://github.com/spf13/cobra/issues/107#issuecomment-482143494
func genZshCompletion(w io.Writer) error {
	tpl := template.Must(template.New("head").Parse(zshHead))
	template.Must(tpl.New("tail").Parse(zshTail))

	if err := tpl.ExecuteTemplate(w, "head", rootCmd); err != nil {
		return err
	}
	if err := rootCmd.GenBashCompletion(w); err != nil {
		return err
	}
	return tpl.ExecuteTemplate(w, "tail", rootCmd)
}

const (
	bashCompletion = `
__kaf_config_use_cluster() {
    if out=$( ./kaf config get-clusters --no-headers ); then
        COMPREPLY=( $( compgen -W "${out[*]}" -- "$cur" ) )
    fi
}

__kaf_topics() {
    if out=$( ./kaf topics --no-headers 2>/dev/null | awk '{print $1}' ); then
        COMPREPLY=( $( compgen -W "${out[*]}" -- "$cur" ) )
    fi
}

__kaf_groups() {
    if out=$( ./kaf groups --no-headers 2>/dev/null | awk '{print $1}' ); then
        COMPREPLY=( $( compgen -W "${out[*]}" -- "$cur" ) )
    fi
}

__kaf_custom_func() {
    case ${last_command} in
        kaf_config_use-cluster)
            __kaf_config_use_cluster
        ;;
        kaf_consume | kaf_produce | kaf_topic_add-config | kaf_topic_delete | kaf_topic_describe)
            __kaf_topics
        ;;
        kaf_group_commit | kaf_group_delete | kaf_group_describe)
            __kaf_groups
        ;;
        *)
        ;;
    esac
}
`

	zshHead = `#compdef {{.Name}}
__cobra_bash_source() {
    alias shopt=':'
    alias _expand=_bash_expand
    alias _complete=_bash_comp
    emulate -L sh
    setopt kshglob noshglob braceexpand
    source "$@"
}
__cobra_type() {
    # -t is not supported by zsh
    if [ "$1" == "-t" ]; then
        shift
        # fake Bash 4 to disable "complete -o nospace". Instead
        # "compopt +-o nospace" is used in the code to toggle trailing
        # spaces. We don't support that, but leave trailing spaces on
        # all the time
        if [ "$1" = "__cobra_compopt" ]; then
            echo builtin
            return 0
        fi
    fi
    type "$@"
}
__cobra_compgen() {
    local completions w
    completions=( $(compgen "$@") ) || return $?
    # filter by given word as prefix
    while [[ "$1" = -* && "$1" != -- ]]; do
        shift
        shift
    done
    if [[ "$1" == -- ]]; then
        shift
    fi
    for w in "${completions[@]}"; do
        if [[ "${w}" = "$1"* ]]; then
            echo "${w}"
        fi
    done
}
__cobra_compopt() {
    true # don't do anything. Not supported by bashcompinit in zsh
}
__cobra_ltrim_colon_completions()
{
    if [[ "$1" == *:* && "$COMP_WORDBREAKS" == *:* ]]; then
        # Remove colon-word prefix from COMPREPLY items
        local colon_word=${1%${1##*:}}
        local i=${#COMPREPLY[*]}
        while [[ $((--i)) -ge 0 ]]; do
            COMPREPLY[$i]=${COMPREPLY[$i]#"$colon_word"}
        done
    fi
}
__cobra_get_comp_words_by_ref() {
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[${COMP_CWORD}-1]}"
    words=("${COMP_WORDS[@]}")
    cword=("${COMP_CWORD[@]}")
}
__cobra_filedir() {
    local RET OLD_IFS w qw
    __cobra_debug "_filedir $@ cur=$cur"
    if [[ "$1" = \~* ]]; then
        # somehow does not work. Maybe, zsh does not call this at all
        eval echo "$1"
        return 0
    fi
    OLD_IFS="$IFS"
    IFS=$'\n'
    if [ "$1" = "-d" ]; then
        shift
        RET=( $(compgen -d) )
    else
        RET=( $(compgen -f) )
    fi
    IFS="$OLD_IFS"
    IFS="," __cobra_debug "RET=${RET[@]} len=${#RET[@]}"
    for w in ${RET[@]}; do
        if [[ ! "${w}" = "${cur}"* ]]; then
            continue
        fi
        if eval "[[ \"\${w}\" = *.$1 || -d \"\${w}\" ]]"; then
            qw="$(__cobra_quote "${w}")"
            if [ -d "${w}" ]; then
                COMPREPLY+=("${qw}/")
            else
                COMPREPLY+=("${qw}")
            fi
        fi
    done
}
__cobra_quote() {
    if [[ $1 == \'* || $1 == \"* ]]; then
        # Leave out first character
        printf %q "${1:1}"
    else
    printf %q "$1"
    fi
}
autoload -U +X bashcompinit && bashcompinit
# use word boundary patterns for BSD or GNU sed
LWORD='[[:<:]]'
RWORD='[[:>:]]'
if sed --help 2>&1 | grep -q GNU; then
    LWORD='\<'
    RWORD='\>'
fi
__{{.Name}}_convert_bash_to_zsh() {
    sed \
    -e 's/declare -F/whence -w/' \
    -e 's/_get_comp_words_by_ref "\$@"/_get_comp_words_by_ref "\$*"/' \
    -e 's/local \([a-zA-Z0-9_]*\)=/local \1; \1=/' \
    -e 's/flags+=("\(--.*\)=")/flags+=("\1"); two_word_flags+=("\1")/' \
    -e 's/must_have_one_flag+=("\(--.*\)=")/must_have_one_flag+=("\1")/' \
    -e "s/${LWORD}_filedir${RWORD}/__cobra_filedir/g" \
    -e "s/${LWORD}_get_comp_words_by_ref${RWORD}/__cobra_get_comp_words_by_ref/g" \
    -e "s/${LWORD}__ltrim_colon_completions${RWORD}/__cobra_ltrim_colon_completions/g" \
    -e "s/${LWORD}compgen${RWORD}/__cobra_compgen/g" \
    -e "s/${LWORD}compopt${RWORD}/__cobra_compopt/g" \
    -e "s/${LWORD}declare${RWORD}/builtin declare/g" \
    -e "s/\\\$(type${RWORD}/\$(__cobra_type/g" \
    <<'BASH_COMPLETION_EOF'
`

	zshTail = `
BASH_COMPLETION_EOF
}
__cobra_bash_source <(__{{.Name}}_convert_bash_to_zsh)
_complete {{.Name}} 2>/dev/null
`
)
