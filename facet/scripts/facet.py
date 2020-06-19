import sys
import click
import facet


CONTEXT_SETTINGS = {
    'max_content_width': 120,
    'allow_extra_args': True,  # enabled allows '&' (background process)
    'ignore_unknown_options': False,
    # 'show_default': False,  # disabled globally to ignore trivial options
    'default_map': {
        'required': False,
    }
}


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=facet.__version__)
def cli():
    pass


@click.command(context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-q', '--query',
    type=str,
    help='Query string/directory/file.',
)
@click.option(
    '-a', '--alpha',
    type=float,
    default=0.7,
    show_default=True,
    help='Similarity threshold.',
)
@click.option(
    '-s', '--similarity',
    type=str,
    default='cosine',
    show_default=True,
    help='Similarity measure.',
)
@click.option(
    '-f', '--format',
    type=str,
    default='json',
    show_default=True,
    help='Format for match results.',
)
@click.option(
    '-o', '--output',
    type=str,
    help='Output target for match results.',
)
@click.option(
    '-t', '--tokenizer',
    type=str,
    default='ws',
    show_default=True,
    help='Tokenizer for text procesing.',
)
def match(query, alpha, similarity, format, output, tokenizer):
    db1 = facet.RedisDatabase(db=0)
    db2 = facet.RedisDatabase(db=1)
    db3 = facet.DictDatabase('db/umls_midsmall', flag='r')
    ss = facet.Simstring(db=db3, alpha=alpha, similarity=similarity)
    f = facet.Facet(
        conso_db=db1,
        cuisty_db=db2,
        simstring=ss,
        tokenizer=tokenizer,
    )
    matches = f.match(query, format=format, outfile=output)
    print(matches)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-q', '--query',
    type=str,
    help='Query string.',
)
@click.option(
    '-a', '--alpha',
    type=float,
    default=0.7,
    show_default=True,
    help='Similarity threshold.',
)
@click.option(
    '-s', '--similarity',
    type=str,
    default='cosine',
    show_default=True,
    help='Similarity measure.',
)
def simstring(query, alpha, similarity):
    db = facet.DictDatabase('db/umls_midsmall', flag='r')
    ss = facet.Simstring(db=db, alpha=alpha, similarity=similarity)
    matches = ss.search(query)
    for k,v in matches:
        print(k,v)


# Organize groups and commands
cli.add_command(match)
cli.add_command(simstring)


def main():
    """Program entry point"""
    return cli()


if __name__ == '__main__':
    main()
