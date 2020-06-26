import sys
import click
from ..utils import load_configuration
from .. import (
    __version__,
    Facet,
    UMLSFacet,
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


def repl_loop(f):
    """REPL loop.

    NOTE:
        * 'output' is not used.
    """
    try:
        while True:
            query_or_option = input('> ')
            if query_or_option == 'exit()':
                f.close()
                break

            # Allow changing some options from the command prompt.
            # An '=' symbol represents an option-value pair.
            if '=' in query_or_option:
                option, value = query_or_option.split('=')
                option = option.strip()
                value = value.strip('\'" ')
                if option == 'alpha':
                    f.simstring.alpha = float(value)
                elif option == 'similarity':
                    f.simstring.similarity = value
                elif option == 'tokenizer':
                    f.tokenizer = value
                elif option == 'format':
                    f.formatter = value
                else:
                    query = query_or_option
                    print(f.match(query))
            else:
                query = query_or_option
                if query == 'alpha()':
                    print(f.simstring.alpha)
                elif query == 'similarity()':
                    print(f.simstring.similarity)
                elif query == 'tokenizer()':
                    print(f.tokenizer)
                elif query == 'format()':
                    print(f.formatter)
                else:
                    print(f.match(query))
    except (KeyboardInterrupt, EOFError):
        print()
        f.close()


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__)
def cli():
    pass


@click.command(context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-c', '--config',
    type=str,
    help='Configuration file.',
)
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
    type=click.Choice(('dict', 'redis', 'elasticsearch')),
    default='dict',
    show_default=True,
    help='Database for Simstring install/query.',
)
@click.option(
    '-i', '--install',
    type=str,
    help='Data file to install (single column file).',
)
def match(
    config,
    query,
    alpha,
    similarity,
    format,
    output,
    tokenizer,
    database,
    install,
):
    f = Facet(
        simstring=Simstring(db=database, alpha=alpha, similarity=similarity),
        tokenizer=tokenizer,
        formatter=format,
    )

    if install:
        f.install(install)

    if query:
        print(f.match(query, outfile=output))
        f.close()
    else:
        repl_loop(f)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-c', '--config',
    type=str,
    help='Configuration file.',
)
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
    type=click.Choice(('dict', 'redis', 'elasticsearch')),
    default='dict',
    show_default=True,
    help='Database for Simstring install/query.',
)
@click.option(
    '-i', '--install',
    type=str,
    help='Data file to install (single column file).',
)
@click.option(
    '--conso_db',
    type=click.Choice(('dict', 'redis')),
    default='dict',
    show_default=True,
    help='Database for CONCEPT-CUI mapping.',
)
@click.option(
    '--cuisty_db',
    type=click.Choice(('dict', 'redis')),
    default='dict',
    show_default=True,
    help='Database for CUI-STY mapping.',
)
def umls(
    config,
    query,
    alpha,
    similarity,
    format,
    output,
    tokenizer,
    database,
    install,
    conso_db,
    cuisty_db,
):
    f = UMLSFacet(
        conso_db=conso_db,
        cuisty_db=cuisty_db,
        simstring=Simstring(db=database, alpha=alpha, similarity=similarity),
        tokenizer=tokenizer,
        formatter=format,
    )

    if install:
        f.install(install)

    if query:
        print(f.match(query, outfile=output))
        f.close()
    else:
        repl_loop(f)


# Organize groups and commands
cli.add_command(match)
cli.add_command(umls)


def main():
    """Program entry point"""
    return cli()


if __name__ == '__main__':
    main()
