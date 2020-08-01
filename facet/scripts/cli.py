import os
import sys
import copy
import click
import signal
import socket
import facet
from typing import (
    Any,
    Dict,
)


CONTEXT_SETTINGS = {
    'max_content_width': 90,
    'allow_extra_args': True,  # enabled allows '&' (background process)
    'ignore_unknown_options': False,
    # 'show_default': False,  # disabled globally to ignore trivial options
    'default_map': {
        'required': False,
    }
}


def load_configuration(ctx, param, config: str):
    return facet.Configuration().load(config)


def parse_dump_configuration(output: str, format='yaml'):
    """Parse argument of dump configuration option.

    A dump configuration option consists of a file and/or format using
    the following syntax:
        * Dump to file using default format - "file"
        * Dump to file with specified format - "file:format"
        * Print to STDOUT with specified format - "format"
    """
    if ':' in output:
        output, format = output.split(':')
    elif output in facet.formatter.formatter_map:
        output, format = None, output
    return output, format


def dump_configuration(config: Dict[str, Any], output=None, format='yaml'):
    """Dump configuration data to a file or STDOUT."""
    # Run configuration through loader so that it gets parsed
    parsed_config = facet.Configuration().load(config)

    formatter = facet.formatter.get_formatter(format)()
    formatted_config = formatter(parsed_config, output=output)
    if formatted_config is not None:
        print(formatted_config)


def repl_loop(obj, prompt_symbol: str = '>'):
    """Read-Evaluate-Print-Loop.

    Args:
        obj (BaseFacet): FACET instance.

        prompt_symbol (str): Symbol for prompt.
    """
    prompt = prompt_symbol + ' '
    try:
        while True:
            query = input(prompt)
            if query == 'exit()':
                break

            # Allow escaping exit command
            if query == '\\exit()':
                query = 'exit()'

            print(obj.match(query))
    except (KeyboardInterrupt, EOFError):
        print()


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass


@click.command(context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-c', '--config',
    type=str,
    callback=load_configuration,
    help=("Configuration file of either 'file' or 'file:section' form."),
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
    default='jaccard',
    show_default=True,
    help='Similarity measure.',
)
@click.option(
    '-n', '--ngram',
    type=click.Choice(('character', 'word')),
    default='character',
    show_default=True,
    help='N-gram feature extractor.',
)
@click.option(
    '-f', '--formatter',
    type=click.Choice(('json', 'yaml', 'xml', 'csv', 'pickle', 'null')),
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
    type=click.Choice(
        ('alphanumeric', 'whitespace', 'symbol', 'nltk', 'spacy', 'null')
    ),
    default='alphanumeric',
    show_default=True,
    help='Tokenizer for text procesing.',
)
@click.option(
    '-d', '--database',
    type=click.Choice(('dict', 'sqlite', 'redis', 'elasticsearch')),
    default='dict',
    show_default=True,
    help='Database for Matcher install/query.',
)
@click.option(
    '-i', '--install',
    type=str,
    help='Data file to install (default is first column of CSV file). '
         'Supports pairs of key=value to pass to install().',
)
@click.option(
    '-g', '--dump_config',
    type=str,
    help='Print configuration and exit. '
         'Option form: "file", "file:format", "format"'
         'Formats supported: json, yaml, xml',
)
def run(
    config,
    query,
    alpha,
    similarity,
    ngram,
    formatter,
    output,
    tokenizer,
    database,
    install,
    dump_config,
):
    # Resolve settings for dumping configuration
    full_config = copy.deepcopy(config)
    dump_config = config.pop('dump_config', dump_config)
    if dump_config:
        dump_output, dump_format = parse_dump_configuration(dump_config)

    # Support CLI shortcut options from configuration files
    query = config.pop('query', query)
    output = config.pop('output', output)
    install = config.pop('install', install)
    if isinstance(install, str):
        install = {'filename': install}

    # Prepare factory options from configuration
    factory_config = copy.deepcopy(config)
    factory_config['class'] = config.get('class', 'facet')
    factory_config['tokenizer'] = config.get('tokenizer', tokenizer)
    factory_config['formatter'] = config.get('formatter', formatter)
    factory_config['matcher'] = config.get('matcher', {
        'class': 'simstring',
        'db': database,
        'alpha': config.get('alpha', alpha),
        'similarity': config.get('similarity', similarity),
        'ngram': config.get('ngram', ngram),
    })

    if dump_config:
        full_config.update(factory_config)
        dump_configuration({'FACET': full_config}, dump_output, dump_format)
        return

    f = facet.FacetFactory(factory_config).create()

    if install:
        f.install(**install)

    if query:
        matches = f.match(query, output=output)
        if matches is not None:
            print(matches)
    else:
        repl_loop(f)

    f.close()


@click.command(context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-c', '--config',
    type=str,
    callback=load_configuration,
    help=("Configuration file of either 'file' or 'file:section' form."),
)
@click.option(
    '-h', '--host',
    type=str,
    default='localhost',
    show_default=True,
    help='Server host.',
)
@click.option(
    '-p', '--port',
    type=click.IntRange(1024, 65535),
    default=4444,
    show_default=True,
    help='Server port.',
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
    default='jaccard',
    show_default=True,
    help='Similarity measure.',
)
@click.option(
    '-n', '--ngram',
    type=click.Choice(('character', 'word')),
    default='character',
    show_default=True,
    help='N-gram feature extractor.',
)
@click.option(
    '-f', '--formatter',
    type=click.Choice(('json', 'yaml', 'xml', 'csv', 'pickle', 'null')),
    default='json',
    show_default=True,
    help='Format for match results.',
)
@click.option(
    '-t', '--tokenizer',
    type=click.Choice(
        ('alphanumeric', 'whitespace', 'symbol', 'nltk', 'spacy', 'null',)
    ),
    default='alphanumeric',
    show_default=True,
    help='Tokenizer for text procesing.',
)
@click.option(
    '-d', '--database',
    type=click.Choice(('dict', 'sqlite', 'redis', 'elasticsearch')),
    default='dict',
    show_default=True,
    help='Database for Matcher install/query.',
)
@click.option(
    '-i', '--install',
    type=str,
    help='Data file to install (default is first column of CSV file). '
         'Supports pairs of key=value to pass to install().',
)
@click.option(
    '-g', '--dump_config',
    type=str,
    help='Print configuration and exit. '
         'Option form: "file", "file:format", "format"'
         'Formats supported: json, yaml, xml',
)
def server(
    config,
    host,
    port,
    query,
    alpha,
    similarity,
    ngram,
    formatter,
    tokenizer,
    database,
    install,
    dump_config,
):
    # Resolve settings for dumping configuration
    full_config = copy.deepcopy(config)
    dump_config = config.pop('dump_config', dump_config)
    if dump_config:
        dump_output, dump_format = parse_dump_configuration(dump_config)

    # Support CLI shortcut options from configuration files
    host = config.pop('host', host)
    port = config.pop('port', port)
    query = config.pop('query', query)
    install = config.pop('install', install)
    if isinstance(install, str):
        install = {'filename': install}

    # Prepare factory options from configuration
    factory_config = copy.deepcopy(config)
    factory_config['class'] = config.get('class', 'facet')
    factory_config['tokenizer'] = config.get('tokenizer', tokenizer)
    factory_config['formatter'] = config.get('formatter', formatter)
    factory_config['matcher'] = config.get('matcher', {
        'class': 'simstring',
        'db': database,
        'alpha': config.get('alpha', alpha),
        'similarity': config.get('similarity', similarity),
        'ngram': config.get('ngram', ngram),
    })

    if dump_config:
        full_config.update(factory_config)
        dump_configuration({'SERVER': full_config}, dump_output, dump_format)
        return

    f = facet.FacetFactory(factory_config).create()

    if install:
        f.install(**install)

    with facet.network.SocketServer(
        (host, port),
        facet.network.SocketServerHandler,
        served_object=f,
    ) as server:
        server.serve_forever()

    f.close()


@click.command(context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-c', '--config',
    type=str,
    callback=load_configuration,
    help=("Configuration file of either 'file' or 'file:section' form."),
)
@click.option(
    '-h', '--host',
    type=str,
    default='localhost',
    show_default=True,
    help='Server host.',
)
@click.option(
    '-p', '--port',
    type=click.IntRange(1024, 65535),
    default=4444,
    show_default=True,
    help='Server port.',
)
@click.option(
    '-q', '--query',
    type=str,
    help='Query string/directory/file.',
)
@click.option(
    '-f', '--formatter',
    type=click.Choice(('json', 'yaml', 'xml', 'csv', 'pickle', 'null')),
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
    '-g', '--dump_config',
    type=str,
    help='Print configuration and exit. '
         'Option form: "file", "file:format", "format"'
         'Formats supported: json, yaml, xml',
)
def client(config, host, port, query, formatter, output, dump_config):
    # Resolve settings for dumping configuration
    full_config = copy.deepcopy(config)
    dump_config = config.pop('dump_config', dump_config)
    if dump_config:
        dump_output, dump_format = parse_dump_configuration(dump_config)

    # Support CLI shortcut options from configuration files
    query = config.pop('query', query)
    host = config.pop('host', host)
    port = config.pop('port', port)
    output = config.pop('output', output)

    # Prepare factory options from configuration
    factory_config = copy.deepcopy(config)
    factory_config['class'] = config.get('class', 'facet')
    factory_config['formatter'] = config.get('formatter', formatter)

    if dump_config:
        full_config.update(factory_config)
        dump_configuration({'CLIENT': full_config}, dump_output, dump_format)
        return

    with facet.network.SocketClient(
        (host, port),
        target_class=facet.facets.get_facet(factory_config['class']),
    ) as f:
        if query:
            matches = f.match(query)
            if matches is not None:
                print(matches)
        else:
            repl_loop(f)


@click.command('server-shutdown', context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-h', '--host',
    type=str,
    default='localhost',
    show_default=True,
    help='Server host.',
)
@click.option(
    '-p', '--port',
    type=click.IntRange(1024, 65535),
    default=4444,
    show_default=True,
    help='Server port.',
)
@click.option(
    '--fileno',
    type=int,
    help='File descriptor/number of server socket (only for local system)',
)
@click.option(
    '--pid',
    type=int,
    help='Process ID of server (only for local system)',
)
def server_shutdown(host, port, fileno, pid):
    """Stop network server."""
    if pid:
        os.kill(pid, signal.SIGKILL)
    elif fileno:
        try:
            socket.close(fileno)
        except AttributeError as ex:
            raise ex(
                'to close server socket with file descriptor/number'
                'requires at least Python 3.7'
            )
        except OSError as ex:
            raise ex('failed to close server socket')
    elif host:
        client = socket.socket(
            family=socket.AF_INET,
            type=(
                (socket.SOCK_STREAM | socket.SOCK_CLOEXEC)
                if sys.platform.startswith('linux')
                else socket.SOCK_STREAM
            )
        )
        try:
            client.connect(facet.parse_address(host, port))
        except OSError as ex:
            raise ex('failed to connect to server')

        try:
            client.sendall(b'shutdown')
        except OSError as ex:
            raise ex('failed to send shutdown command to server')

        client.close()


# Organize groups and commands
cli.add_command(run)
cli.add_command(server)
cli.add_command(client)
cli.add_command(server_shutdown)


def main():
    """Program entry point"""
    return cli()


if __name__ == '__main__':
    main()
