# Autocompletion for FACET via Click
#   https://click.palletsprojects.com/en/7.x/bashcomplete/#activation
#   https://click.palletsprojects.com/en/7.x/bashcomplete/#activation-script
#
# Source this script manually or from your .<shell>rc file:
#   . autocompletion.sh
#   cat autocompletion.sh >> ~/.<shell>rc


# Bash (~/.bashrc)
eval "$(_FACET_COMPLETE=source_bash facet)"

# Zsh (~/.zshrc)
#eval "$(_FACET_COMPLETE=source_zsh facet)"

# Fish (~/.config/fish/completions/facet.fish)
#eval (env _FACET_COMPLETE=source_fish facet)
