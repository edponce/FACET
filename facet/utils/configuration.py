import os
import re
import yaml
import json
import urllib
import pyparsing
import configparser


# Boolean and None states are case-insensitive
BOOLEAN_STATES = {
    True: ('true', 'yes', 'on', 'accept', 'enable'),
    False: ('false', 'no', 'off', 'reject', 'disable'),
}

NONE_STATES = ('none', 'null', 'nul', 'nil')

LEFT_BRACKETS = '([{<'
RIGHT_BRACKETS = ')]}>'
LPAREN, LBRACK, LBRACE, LANGLE = map(pyparsing.Suppress, LEFT_BRACKETS)
RPAREN, RBRACK, RBRACE, RANGLE = map(pyparsing.Suppress, RIGHT_BRACKETS)
BRACKETS = LEFT_BRACKETS + RIGHT_BRACKETS

QUOTES = '\'\"'
SGLQUOTE, DBLQUOTE = map(pyparsing.Suppress, QUOTES)

DELIMITERS = ':,='
COLON, COMMA, EQUAL = map(pyparsing.Suppress, DELIMITERS)

PUNCTUATIONS = BRACKETS + QUOTES + DELIMITERS

# Integral, real, and scientific numbers
numberToken = pyparsing.pyparsing_common.number

# Boolean values
boolToken = pyparsing.oneOf(
    ' '.join(BOOLEAN_STATES[True] + BOOLEAN_STATES[False]),
    caseless=True,
    asKeyword=True,
)
boolToken.setParseAction(lambda token: token[0] in BOOLEAN_STATES[True])

# None values
noneToken = pyparsing.oneOf(
    ' '.join(NONE_STATES),
    caseless=True,
    asKeyword=True,
)
noneToken.setParseAction(pyparsing.replaceWith(None))

# Quoted strings
quotedToken = pyparsing.quotedString(QUOTES)
quotedToken.setParseAction(pyparsing.removeQuotes)

# Unquoted strings
rawToken = pyparsing.Word(pyparsing.printables, excludeChars=PUNCTUATIONS)

# Key/value pairs
kvToken = pyparsing.Forward()

# Iterables: key/value, list, tuple, dict, set
kvIterToken = pyparsing.Forward()
listToken = pyparsing.Forward()
tupleToken = pyparsing.Forward()
dictToken = pyparsing.Forward()
setToken = pyparsing.Forward()

# Parsers: scalar and all
# Order matters based on the following rules:
#   Key/value tokens are first literals
#   Numbers before other scalar literals
#   Iterables are last literals
#   bool/none before raw/quoted to prioritize keyword comparison
#   kvIterToken as first iterable to prioritize kvToken comparison
#   dictToken before setToken to resolve '{}' as a dictionary
pyparser_scalars = (
    numberToken
    | boolToken
    | noneToken
    | quotedToken
    | rawToken
)
pyparser = (
    kvToken
    | pyparser_scalars
    | kvIterToken
    | listToken
    | tupleToken
    | dictToken
    | setToken
)

# Key/value pairs: '[key1=val1, key2=val2, ...]'
# Key can only be scalar literals
kvToken <<= pyparsing.Group(pyparser_scalars + EQUAL + pyparser)
kvToken.setParseAction(lambda token: dict(token.asList()))
kvIterToken <<= (
    (
        LPAREN
        + pyparsing.delimitedList(kvToken)
        + pyparsing.Optional(COMMA)
        + RPAREN
    ) ^ (
        LBRACK
        + pyparsing.delimitedList(kvToken)
        + pyparsing.Optional(COMMA)
        + RBRACK
    ) ^ (
        LBRACE
        + pyparsing.delimitedList(kvToken)
        + pyparsing.Optional(COMMA)
        + RBRACE
    ) ^ (
        LANGLE
        + pyparsing.delimitedList(kvToken)
        + pyparsing.Optional(COMMA)
        + RANGLE
    )
)
kvIterToken.setParseAction(lambda token: {
    k: v
    for d in token.asList()
    for k, v in d.items()
})

listToken <<= (
    LBRACK
    + pyparsing.Optional(pyparsing.delimitedList(pyparser))
    + pyparsing.Optional(COMMA)
    + RBRACK
)
listToken.setParseAction(lambda token: [token.asList()])

# Tuples: '(val1, ...)' or '<val1, ...>'
tupleToken <<= (
    (
        LPAREN
        + pyparsing.Optional(pyparsing.delimitedList(pyparser))
        + pyparsing.Optional(COMMA)
        + RPAREN
    ) ^ (
        LANGLE
        + pyparsing.Optional(pyparsing.delimitedList(pyparser))
        + pyparsing.Optional(COMMA)
        + RANGLE
    )
)
tupleToken.setParseAction(lambda token: tuple(token))

dictEntry = pyparsing.Group(pyparser + COLON + pyparser)
dictToken <<= (
    LBRACE
    + pyparsing.Optional(pyparsing.delimitedList(dictEntry))
    + pyparsing.Optional(COMMA)
    + RBRACE
)
dictToken.setParseAction(lambda token: dict(token.asList()))

setToken <<= (
    LBRACE
    + pyparsing.Optional(pyparsing.delimitedList(pyparser))
    + pyparsing.Optional(COMMA)
    + RBRACE
)
setToken.setParseAction(lambda token: set(token))

# Non-enclosed, non-quoted CSV
rawCSVToken = (
    pyparsing.Optional(pyparser)
    + COMMA
    + pyparsing.Optional(pyparser)
)


def parse_string(string, protect_numerics=False):
    # Get copy of original string, to return it in case of invalid/error
    orig_string = string

    # Case: Protect string numerics --> Do not parse
    # NOTE: Used only for YAML/JSON file configurations because their loaders
    # parse the data.
    if protect_numerics:
        try:
            float(string)
        except ValueError:
            pass
        else:
            return string

    # Case: Non-enclosed CSV --> Convert to a tuple
    try:
        rawCSVToken.parseString(string)
    except pyparsing.ParseException:
        pass
    else:
        string = '(' + string + ')'

    try:
        return pyparser.parseString(string, parseAll=True)[0]
    except pyparsing.ParseException:
        print('WARNING: configuration parser failed to parse', orig_string)
        return orig_string


def parse(obj, **kwargs):
    """Parse arbitrary objects."""
    if isinstance(obj, (str, bytes)):
        return parse_string(obj, **kwargs)
    else:
        if isinstance(obj, dict):
            # NOTE: Keys are not modified
            return {k: parse(v, **kwargs) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple, set)):
            return type(obj)([parse(v, **kwargs) for v in obj])
        else:
            return obj


def load_configuration_from_string(data):
    """Parse a string into a dictionary.

    Returns:
        dict[str:Any]: Configuration mapping
        None: If error/invalid occurs during parsing
    """
    data = data.strip()
    if (
        # Case: '{key1:value, key2:value, ...}'
        data[0] == '{' and data[-1] == '}'
        # Case: 'key1=value, key2=value,...'
        or '=' in data
    ):
        return parse(data)


def load_configuration_from_file(filename, file_type=None):
    """Load configuration data from YAML/JSON/INI file.

    Args:
        file_type (str): File format to consider for file, ignore extension.

    Returns:
        dict[str:Any]: Configuration mapping
        None: If error/invalid occurs during parsing
    """
    # Get file extension/format
    if not file_type:
        _, ext = os.path.splitext(filename)
        ext = ext[1:].lower()
    else:
        ext = file_type.lower()

    # YAML file
    if ext in ('yaml', 'yml'):
        with open(filename) as fd:
            return parse(yaml.safe_load(fd), protect_numerics=True)

    # JSON file
    elif ext == 'json':
        with open(filename) as fd:
            try:
                return parse(json.load(fd), protect_numerics=True)
            except json.decoder.JSONDecodeError:
                return load_configuration_from_string(fd.read())

    # INI file
    elif ext in ('ini', 'cfg', 'conf'):
        parser = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation(),
        )

        # Leave options (keys) unchanged, do not lowercase
        parser.optionxform = lambda option: option

        with open(filename) as fd:
            parser.read_file(fd, source=filename)

        config = {
            section: dict(parser[section])
            for section in parser.sections()
        }
        if len(parser.defaults()) > 0:
            config[parser.default_section] = dict(parser.defaults())

        return parse(config)


def load_configuration(data, key=None, file_type=None):
    """Load configuration from file/string.

    Args:
        data (str|dict): Configuration file/data to parse.
            File can embed key via format: 'file.conf:key'
            Embedded key has priority over key parameter.

        key (str): Extract only data from configuration corresponding to key

        file_type (str): File format to consider for file, ignore extension.

    Returns:
        dict[str:Any]: Configuration mapping
        None: If error/invalid occurs during parsing
    """
    if isinstance(data, (str, bytes)):
        # Check if key is provided with filename
        file_key = data.split(':', 1)
        if len(file_key) > 1 and os.path.isfile(file_key[0]):
            data, key = file_key

        # YAML/JSON/INI file
        if os.path.isfile(data):
            config = load_configuration_from_file(data, file_type=file_type)
        else:
            config = load_configuration_from_string(data)

    elif isinstance(data, dict):
        config = data

    elif data is None:
        return {}

    # If configuration is not a mapping, then invalid/error parsing
    # NOTE: Configuration should be parsed before this check.
    if not isinstance(config, dict):
        return None

    # Filter specific key. If key is not available, then mapping is empty.
    # Parse mapping values afterwards.
    if config and key is not None:
        config = config.get(key, {})

    return config


def parse_address(address):
    """Parses (in a best-effort manner) variants of hostnames and URLs,
    to extract host and port.

    Examples:
        * localhost
        * 127.0.0.1
        * localhost:9020
        * http://username@localhost:9020

    Returns:
        (host, port): Hostname and port.
    """
    parsed = urllib.parse.urlsplit(address)
    # NOTE: urllib does not parse address if it does not contain a protocol.
    # The 'path' attribute gets the value of address and 'netloc' is empty.
    if not parsed.netloc:
        # Sanitize start of address (remove protocol delimiters)
        match = re.search(r'^[:|/]*(.*)', address)
        if match:
            address = match.group(0)
        parsed = urllib.parse.urlsplit('http://' + address)

    host = parsed.hostname
    # urllib triggers error if port is not available
    try:
        return host, parsed.port
    except ValueError:
        return host, None


def unparse_address(
    host,
    port=None,
    protocol='http',
    user=None,
    password=None,
    path=None,
):
    protocol = protocol + '://' if protocol else ''
    port = ':' + str(port) if port else ''  # port 0 is not allowed
    credentials = (
        user + (':' + password if password else '') + '@' if user else ''
    )
    path = (path if path[0] == '/' else '/' + path) if path else ''
    return protocol + credentials + host + port + path
