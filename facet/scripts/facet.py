import sys
import click
from ..utils import load_configuration
from .. import (
    __version__,
    Facet,
    UMLSFacet,
    RedisDatabase,
    DictDatabase,
    Simstring,
)


CONTEXT_SETTINGS = {
    'max_content_width': 120,
    'allow_extra_args': True,  # enabled allows '&' (background process)
    'ignore_unknown_options': False,
    # 'show_default': False,  # disabled globally to ignore trivial options
    'default_map': {
        'required': False,
    }
}


def facet_config(ctx, param, value):
    """Set up configuration.

    Parameters supported:
        * YAML/JSON/INI file
        * dict/key=value string

    Returns:
        dict[str,Any]: Configuration mapping.
    """
    if not value:
        return {}

    config = load_configuration(value, key='FACET')
    if config is None:
        print('ERROR: invalid --config parameter', file=sys.stderr)
        sys.exit(0)
    elif len(config) == 0:
        print('WARNING: unable to find configuration data for '
              '--config parameter', file=sys.stderr)
    return config


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__)
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
    type=click.Choice(('dice', 'exact', 'cosine', 'jaccard', 'overlap',
                       'hamming')),
    default='cosine',
    show_default=True,
    help='Similarity measure.',
)
@click.option(
    '-f', '--format',
    type=click.Choice(('json', 'yaml', 'xml', 'pickle', 'csv')),
    default=None,
    help='Format for match results.',
)
@click.option(
    '-o', '--output',
    type=str,
    help='Output target for match results.',
)
@click.option(
    '-t', '--tokenizer',
    type=click.Choice(('ws', 'nltk', 'spacy')),
    default=None,
    help='Tokenizer for text procesing.',
)
@click.option(
    '-d', '--database',
    type=click.Choice(('dict', 'redis')),
    default='dict',
    show_default=True,
    help='Database to connect to for install/query.',
)
@click.option(
    '-i', '--install',
    type=str,
    help='Data file to install (first column).',
)
def match(
    query,
    alpha,
    similarity,
    format,
    output,
    tokenizer,
    database,
    install,
):
    ss = Simstring(db=database, alpha=alpha, similarity=similarity)
    f = Facet(simstring=ss, tokenizer=tokenizer, formatter=format)

    if install:
        f.install(install)

    if query:
        matches = f.match(query, outfile=output)
        print(matches)
    else:
        print('REPL loop')


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
    pass
    # db = DictDatabase('db/umls_midsmall', flag='r')
    # ss = Simstring(db=db, alpha=alpha, similarity=similarity)
    # matches = ss.search(query)
    # for k, v in matches:
    #     print(k, v)


# Organize groups and commands
cli.add_command(match)
cli.add_command(simstring)


def main():
    """Program entry point"""
    return cli()


if __name__ == '__main__':
    main()
