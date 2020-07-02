import os
import sys
import click
import signal
import socket
from functools import partial
from ..utils import (
    load_configuration,
    parse_address,
)
from .. import __version__
from ..network import (
    SocketClient,
    SocketServer,
    SocketServerHandler,
)
from ..facets import facet_map
from ..factory import FacetFactory


CONTEXT_SETTINGS = {
    'max_content_width': 120,
    'allow_extra_args': True,  # enabled allows '&' (background process)
    'ignore_unknown_options': False,
    # 'show_default': False,  # disabled globally to ignore trivial options
    'default_map': {
        'required': False,
    }
}


def facet_config(ctx, param, value, key=None):
    """Set up configuration."""
    return load_configuration(value, keys=key)


def repl_loop(f, *, enable_cmds: bool = True, prompt_symbol: str = '>'):
    """Read-Evaluate-Print-Loop.

    Args:
        f (BaseFacet): FACET instance.

        enable_cmds (bool): If set, get/set commands are supported alongside
            matching queries. Commands allow changing some options from the
            command prompt. A '=' symbol represents an option=value pair.
    """
    prompt = prompt_symbol + ' '
    try:
        while True:
            query_or_cmd = input(prompt)
            if not enable_cmds:
                print(f.match(query_or_cmd))
                continue

            if query_or_cmd == 'exit()':
                break

            try:
                if '=' in query_or_cmd:
                    option, value = query_or_cmd.split('=')
                    option = option.strip()
                    value = value.strip('\'" ')
                    if option == 'alpha':
                        f.simstring.alpha = float(value)
                    elif option == 'similarity':
                        f.simstring.similarity = value
                    elif option == 'tokenizer':
                        f.tokenizer = value
                    elif option == 'formatter':
                        f.formatter = value
                    else:
                        print(f.match(query_or_cmd))
                else:
                    query = query_or_cmd
                    if query == 'help()':
                        print('Commands:')
                        print('  alpha, similarity, tokenizer, formatter')
                        print()
                        print('Get format: cmd()')
                        print('Set format: cmd = value')
                    elif query == 'alpha()':
                        print(f.simstring.alpha)
                    elif query == 'similarity()':
                        print(f.simstring.similarity)
                    elif query == 'tokenizer()':
                        print(f.tokenizer)
                    elif query == 'formatter()':
                        print(f.formatter)
                    else:
                        print(f.match(query))
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
    callback=partial(facet_config, key='match'),
    help='Configuration file.',
)
@click.option(
    '-q', '--query',
    type=str,
    help='Query string/directory/file.',
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
    type=click.Choice(('simple', 'ws', 'nltk', 'spacy')),
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
    help='Data file to install (default is first column of CSV file). '
         'Supports pairs of key=value to pass to install().',
)
@click.option(
    '-m', '--mode',
    type=click.Choice(('server', 'client')),
    help='Run as a server.',
)
def match(
    config,
    query,
    host,
    port,
    alpha,
    similarity,
    formatter,
    output,
    tokenizer,
    database,
    install,
    mode,
):
    factory_config = {}
    factory_config['class'] = config.pop('class', 'facet')
    factory_config['tokenizer'] = config.pop('tokenizer', tokenizer)
    factory_config['formatter'] = config.pop('formatter', formatter)
    factory_config['simstring'] = config.pop('simstring', {
        'class': 'simstring',
        'db': config.pop('database', database),
        'alpha': config.pop('alpha', alpha),
        'similarity': config.pop('similarity', similarity),
    })

    query = config.pop('query', query)
    host = config.pop('host', host)
    port = config.pop('port', port)
    output = config.pop('output', output)
    install = config.pop('install', install)
    if isinstance(install, str):
        install = {'data': install}
    mode = config.pop('mode', mode)

    if len(config) > 0:
        factory_config.update(config)

    if host:
        host, _port = parse_address(host)
        if _port:
            port = _port

    if mode == 'client':
        f = SocketClient(
            facet_map[factory_config['class']],
            host=host,
            port=port,
        )
        if query:
            print(f.match(
                query,
                output=output,
                format=factory_config['formatter'],
            ))
        else:
            repl_loop(f)
    else:
        factory = FacetFactory(factory_config)
        f = factory.create()

        if install:
            f.install(**install)

        if mode is None:
            if query:
                print(f.match(query, output=output))
            else:
                repl_loop(f)
        elif mode == 'server':
            with SocketServer(
                (host, port),
                SocketServerHandler,
                served_object=f,
            ) as server:
                server.serve_forever()

        f.close()


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
cli.add_command(shutdown_server)


def main():
    """Program entry point"""
    return cli()


if __name__ == '__main__':
    main()
