import os
import yaml
import json
import urllib.parse
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
        return orig_string


def parse(obj, **kwargs):
    """Parse arbitrary objects."""
    if isinstance(obj, str):
        return parse_string(obj, **kwargs)
    elif isinstance(obj, dict):
        # NOTE: Keys are not modified
        return {k: parse(v, **kwargs) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return type(obj)([parse(v, **kwargs) for v in obj])
    else:
        return obj


def parse_with_configparser(config):
    """Parse using configparser.

    Args:
        config (str, Dict[str, Any]): Configuration data/file to parse.
    """
    parser = configparser.ConfigParser(
        delimiters=('=',),
        # Support None type produced by JSON/YAML parsers
        allow_no_value=True,
        interpolation=configparser.ExtendedInterpolation(),
    )

    # Leave options (keys) unchanged, do not lowercase
    parser.optionxform = lambda option: option

    # Parse data
    if isinstance(config, str):
        if os.path.isfile(config):
            with open(config) as fd:
                parser.read_file(fd, source=config)
        else:
            parser.read_string(config)
    elif isinstance(config, dict):
        parser.read_dict(config)
    else:
        raise ValueError('invalid configuration data/file for configparser')

    # Convert ConfigParser to dict (if available, include defaults)
    config = {
        section: dict(parser[section])
        for section in parser.sections()
    }
    if len(parser.defaults()) > 0:
        config[parser.default_section] = dict(parser.defaults())

    return config


def load_configuration_from_string(data):
    """Parse a string into a dictionary.

    Returns:
        dict[str:Any]: Configuration mapping
        None: If error/invalid occurs during parsing
    """
    data = data.strip()
    try:
        config = json.loads(data)
    except json.decoder.JSONDecodeError:
        try:
            config = yaml.safe_load(data)
        except yaml.parser.ParserError:
            config = data

    try:
        return parse_with_configparser(config)
    except Exception:
        return config


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
        file_type = ext[1:]
    file_type = file_type.lower()

    if file_type in ('yaml', 'yml'):
        with open(filename) as fd:
            config = yaml.safe_load(fd)
    elif file_type in ('json',):
        with open(filename) as fd:
            config = json.load(fd)
    elif file_type in ('ini', 'cfg', 'conf'):
        config = filename
    else:
        raise ValueError('invalid configuration file')

    return parse_with_configparser(config)


def load_configuration(data, keys=None, file_type=None, delimiter=':'):
    """Load configuration from file/string.

    Args:
        data (str, dict): Configuration file/data to parse.
            File can embed keys via format: 'file.conf:key1:key2:...'
            Embedded keys supersede parameter keys.

        keys (str, list[str]): Extract only data from configuration
            corresponding to key. If multiple keys are provided, they
            are used (in order) to update the resulting configuration.

        file_type (str): File format to consider for file, ignore extension.

        delimiter (str): Delimiter symbol between file name and keys.

    Returns:
        dict[str:Any]: Configuration mapping
    """
    if isinstance(data, str):
        file_keys = data.split(delimiter)
        if os.path.isfile(file_keys[0]):
            data, *tmp_keys = file_keys
            # Embedded keys supersede parameter keys
            if len(tmp_keys) > 0:
                keys = tmp_keys
            config = load_configuration_from_file(data, file_type=file_type)
        else:
            config = load_configuration_from_string(data)
    elif isinstance(data, dict):
        config = data
    elif data is None:
        return {}
    else:
        raise ValueError('invalid configuration data type')

    # Normalize strings and types based on a grammar
    config = parse(config)

    # Filter data based on keys.
    if keys:
        if isinstance(keys, str):
            keys = [keys]

        tmp_config = {}
        for key in keys:
            tmp_config.update(config[key])
        config = tmp_config

    return config


def parse_address(address, port=None):
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
    # NOTE: urllib needs a scheme to identify 'netloc'.
    # The 'path' attribute gets the value of address and 'netloc' is empty.
    parsed = urllib.parse.urlsplit(address)
    if not parsed.netloc:
        parsed = urllib.parse.urlsplit('http://' + address)

    host = parsed.hostname

    # urllib may trigger error if 'port' value is not available.
    # urllib does not trigger error always and returns None.
    try:
        _port = parsed.port
    except ValueError:
        _port = None

    if _port is not None:
        port = _port

    return host, port


def unparse_address(
    host,
    port=None,
    scheme='http',
    user=None,
    password=None,
):
    """Build a qualified URL from individual parts."""
    credentials = (
        user + (':' + password if password else '') + '@' if user else ''
    )
    host, port = parse_address(host, port)
    port = ':' + str(port) if port else ''  # port 0 is not allowed
    netloc = credentials + host + port
    return urllib.parse.urlunsplit((scheme, netloc, '', '', ''))


def parse_address_query(query: str):
    return dict(
        filter(
            lambda x: all(x),
            map(lambda x: urllib.parse.splitvalue(x), query.split('&')))
    )


def unparse_address_query(query: dict):
    return '&'.join(map(lambda x: '='.join(x), query.items()))


def parse_filename(filename):
    fdir, fname = os.path.split(filename)
    fdir = os.path.abspath(fdir) if fdir else os.getcwd()
    return fdir, fname
