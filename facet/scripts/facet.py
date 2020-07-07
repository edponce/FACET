import os
import sys
import copy
import click
import signal
import socket
import functools
from ..utils import (
    load_configuration,
    parse_address,
    get_obj_map_key,
)
from .. import __version__
from ..network import (
    SocketClient,
    SocketServer,
    SocketServerHandler,
)
from ..facets import facet_map
from ..factory import FacetFactory
from ..formatter import formatter_map
from ..tokenizer import tokenizer_map
from ..matcher.similarity import similarity_map
from typing import (
    Any,
    Dict,
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


DEFAULT_SECTION = 'FACET'


def load_config(ctx, param, config: str, key=None):
    return load_configuration(config, keys=key)


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
    elif output in formatter_map:
        output, format = None, output
    return output, format


def dump_configuration(config: Dict[str, Any], output=None, format='yaml'):
    """Dump configuration data to a file or STDOUT."""
    # Run configuration through loader so that it gets parsed
    parsed_config = load_configuration(config)

    formatter = formatter_map[format]()
    formatted_config = formatter(parsed_config, output=output)
    if formatted_config is not None:
        print(formatted_config)


def repl_loop(obj, *, enable_cmds: bool = True, prompt_symbol: str = '>'):
    """Read-Evaluate-Print-Loop.

    Args:
        obj (BaseFacet): FACET instance.

        enable_cmds (bool): If set, get/set commands are supported alongside
            matching queries. Commands allow changing some options from the
            command prompt. A '=' symbol represents an option=value pair.
    """
    prompt = prompt_symbol + ' '
    try:
        while True:
            query_or_cmd = input(prompt)
            if not enable_cmds:
                print(obj.match(query_or_cmd))
                continue

            if query_or_cmd == 'exit()':
                break

            try:
                if '()' in query_or_cmd:
                    query = query_or_cmd
                    if query == 'help()':
                        print('Commands:')
                        print('  alpha, similarity, tokenizer, formatter')
                        print()
                        print('Get syntax: cmd()')
                        print('Set syntax: cmd = value')
                    elif query == 'alpha()':
                        print(obj.matcher.alpha)
                    elif query == 'similarity()':
                        print(get_obj_map_key(obj.matcher.similarity,
                                              similarity_map))
                    elif query == 'tokenizer()':
                        print(get_obj_map_key(obj.tokenizer, tokenizer_map))
                    elif query == 'formatter()':
                        print(get_obj_map_key(obj.formatter, formatter_map))
                    else:
                        print(obj.match(query))
                elif '=' in query_or_cmd:
                    option, value = query_or_cmd.split('=')
                    option = option.strip()
                    value = value.strip('\'" ')
                    if option == 'alpha':
                        obj.matcher.alpha = float(value)
                    elif option == 'similarity':
                        obj.matcher.similarity = value
                    elif option == 'tokenizer':
                        obj.tokenizer = value
                    elif option == 'formatter':
                        obj.formatter = value
                    else:
                        print(obj.match(query_or_cmd))
                else:
                    print(obj.match(query_or_cmd))
            except AttributeError as ex:
                print('Current mode does not supports commands')
    except (KeyboardInterrupt, EOFError):
        print()


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__)
def cli():
    pass


@click.command(context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-c', '--config',
    type=str,
    callback=functools.partial(load_config, key=DEFAULT_SECTION),
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
    default='jaccard',
    show_default=True,
    help='Similarity measure.',
)
@click.option(
    '-f', '--formatter',
    type=click.Choice(('json', 'yaml', 'xml', 'pickle', 'csv')),
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
    type=click.Choice(('basic', 'ws', 'nltk', 'spacy')),
    default='ws',
    show_default=True,
    help='Tokenizer for text procesing.',
)
@click.option(
    '-d', '--database',
    # type=click.Choice(('dict', 'redis', 'elasticsearch')),
    type=str,
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
def match(
    config,
    query,
    alpha,
    similarity,
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
        install = {'data': install}

    # Prepare factory options from configuration
    factory_config = copy.deepcopy(config)
    factory_config['class'] = config.get('class', 'facet')
    factory_config['tokenizer'] = config.get('tokenizer', tokenizer)
    factory_config['formatter'] = config.get('formatter', formatter)
    factory_config['matcher'] = config.get('matcher', {
        'class': 'simstring',
        'db': config.get('database', database),
        'alpha': config.get('alpha', alpha),
        'similarity': config.get('similarity', similarity),
    })

    if dump_config:
        full_config.update(factory_config)
        dump_configuration(
            {DEFAULT_SECTION: full_config},
            dump_output,
            dump_format,
        )
        return

    f = FacetFactory(factory_config).create()

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
    callback=load_config,
    help='Configuration file.',
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
    '-f', '--formatter',
    type=click.Choice(('json', 'yaml', 'xml', 'pickle', 'csv')),
    default='json',
    show_default=True,
    help='Format for match results.',
)
@click.option(
    '-t', '--tokenizer',
    type=click.Choice(('basic', 'ws', 'nltk', 'spacy')),
    default='ws',
    show_default=True,
    help='Tokenizer for text procesing.',
)
@click.option(
    '-d', '--database',
    type=click.Choice(('dict', 'redis', 'elasticsearch')),
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
        install = {'data': install}

    # Prepare factory options from configuration
    factory_config = copy.deepcopy(config)
    factory_config['class'] = config.get('class', 'facet')
    factory_config['tokenizer'] = config.get('tokenizer', tokenizer)
    factory_config['formatter'] = config.get('formatter', formatter)
    factory_config['matcher'] = config.get('matcher', {
        'class': 'simstring',
        'db': config.get('database', database),
        'alpha': config.get('alpha', alpha),
        'similarity': config.get('similarity', similarity),
    })

    if dump_config:
        full_config.update(factory_config)
        dump_configuration(
            {DEFAULT_SECTION: full_config},
            dump_output,
            dump_format,
        )
        return

    f = FacetFactory(factory_config).create()

    if install:
        f.install(**install)

    with SocketServer(
        (host, port),
        SocketServerHandler,
        served_object=f,
    ) as server:
        server.serve_forever()

    f.close()


@click.command(context_settings=CONTEXT_SETTINGS)
@click.help_option(show_default=False)
@click.option(
    '-c', '--config',
    type=str,
    callback=load_config,
    help='Configuration file.',
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
    type=click.Choice(('json', 'yaml', 'xml', 'pickle', 'csv')),
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
        dump_configuration(
            {DEFAULT_SECTION: full_config},
            dump_output,
            dump_format,
        )
        return

    with SocketClient(
        (host, port),
        target_class=facet_map[factory_config['class']],
    ) as f:
        if query:
            matches = f.match(query)
            if matches is not None:
                print(matches)
        else:
            repl_loop(f)


@click.command('shutdown-server', context_settings=CONTEXT_SETTINGS)
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
def shutdown_server(host, port, fileno, pid):
    """Stop network server."""
    if pid:
        os.kill(pid, signal.SIGKILL)
    elif fileno:
        if sys.version_info < (3, 7):
            print('To close server socket with file descriptor/number'
                  'requires at least Python 3.7')
        else:
            try:
                socket.close(fileno)
            except OSError as ex:
                print('failed to close server socket:', ex, file=sys.stderr)
    elif host:
        # Port embedded with 'host' parameter has priority over 'port'
        host, _port = parse_address(host)
        if _port:
            port = _port

        client = socket.socket(
            family=socket.AF_INET,
            type=socket.SOCK_STREAM | socket.SOCK_CLOEXEC,
        )
        try:
            client.connect((host, port))
            try:
                client.sendall(b'shutdown')
            except OSError as ex:
                print('failed to send shutdown command to server:', ex,
                      file=sys.stderr)
        except OSError as ex:
            print('failed to connect to server:', ex, file=sys.stderr)
        finally:
            try:
                client.close()
            except OSError as ex:
                print('failed to close client socket:', ex, file=sys.stderr)


# Organize groups and commands
cli.add_command(match)
cli.add_command(server)
cli.add_command(client)
cli.add_command(shutdown_server)


def main():
    """Program entry point"""
    return cli()


if __name__ == '__main__':
    main()
