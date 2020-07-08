# Shell completion for FACET via Click
#   https://click.palletsprojects.com/en/7.x/bashcomplete/#activation
#   https://click.palletsprojects.com/en/7.x/bashcomplete/#activation-script
#
# Source this script manually or from your .<shell>rc file:
#   . shell_completion.sh
#   cat shell_completion.sh >> ~/.<shell>rc

if [ "$(command -v readlink)" ]; then
    shell=$(basename "$(readlink -f $SHELL)")
else
    shell=$SHELL
fi

case $shell in
    *zsh*) # Zsh (~/.zshrc)
        eval "$(_FACET_COMPLETE=source_zsh facet)"
        ;;
    *fish*) # Fish (~/.config/fish/completions/facet.fish)
        eval "(env _FACET_COMPLETE=source_fish facet)"
        ;;
    *bash* | *sh) # Bash (~/.bashrc)
        eval "$(_FACET_COMPLETE=source_bash facet)"
        ;;
    *) echo "Shell completion does not supports current shell, $shell"
        ;;
esac
