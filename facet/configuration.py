import os
import re
import sys
import copy
import yaml
import json
import pyparsing
import configparser
from typing import (
    Any,
    Dict,
    Union,
    Iterable,
)


__all__ = ['Configuration']


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


class Configuration:

    def __init__(self):
        self.regex = r'\${{([^}]+)}}'

    def expand_envvars(self, obj):
        """Recursively expand user and environment variables in common
        data structures."""
        # NOTE: To support values of the forms key:value and key=value pairs,
        # 'parse_string' does not recurses, so let the result be checked for
        # instances of data structures that need subsequent parsing.
        if isinstance(obj, str):
            obj = os.path.expandvars(os.path.expanduser(obj))

        if isinstance(obj, dict):
            # NOTE: Keys are not modified
            obj = {k: self.expand_envvars(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple, set)):
            obj = type(obj)([self.expand_envvars(v) for v in obj])

        return obj

    def interpolate(self, obj: dict):
        """Recursively interpolate dictionary values based on a regular
        expression.

        Dictionary is traversed in a depth-first manner and supports
        out-of-order chained interpolations.

        Interpolation values are represented by keys enclosed in '${{...}}'.
        An arbitrary number of consecutive keys can be specified by delimiting
        them with a colon, ':'. Note that keys can be strings for dictionaries
        or indices for indexable iterables.

        Example:
            ${{key}} - substitute current value with dict[key]
            ${{key1:key2}} - substitute current value with dict[key1][key2]
            ${{key:2}} - substitute current value with dict[key][2]
        """
        regex = re.compile(self.regex)

        # Get handle to root of object, so that we can do a forward search
        # for interpolation.
        root = obj

        def _interpolate(obj: dict):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    obj[k] = _interpolate(v)
            elif isinstance(obj, (list, tuple, set)):
                obj = type(obj)([_interpolate(v) for v in obj])
            elif isinstance(obj, str):
                match = regex.fullmatch(obj)
                if match:
                    # Do not modify handle to root
                    value = root
                    for key in match.group(1).split(':'):
                        if isinstance(value, dict):
                            value = value[key]
                        else:
                            value = value[int(key)]
                    obj = copy.deepcopy(_interpolate(value))
            return obj
        return _interpolate(obj)

    def parse_string(self, string, protect_numerics=True):
        """Parse a string based on a grammar and resolve environment variables.

        Args:
            protect_numerics (bool): If set, string-based numbers are not
                parsed to numeric types.
        """
        # Get copy of original string, to return it in case of invalid/error
        orig_string = string

        # Case: Protect string numerics (do not parse)
        if protect_numerics:
            try:
                float(string)
            except ValueError:
                pass
            else:
                return string

        # Case: Expand user and environment variables
        # NOTE: Even though environment variables should have been resolved
        # prior to these parsing actions, for values with single-line composite
        # values (e.g., CSV of key=value pairs), we resolve those internal
        # values as well.
        string = self.expand_envvars(string)

        # Case: Non-enclosed CSV --> Convert to a tuple
        try:
            rawCSVToken.parseString(string)
        except pyparsing.ParseException:
            pass
        else:
            string = '(' + string + ')'

        try:
            # NOTE: Parser does not parses correctly a value that begins with
            # numbers followed by other characters, e.g., value=2a.
            return pyparser.parseString(string, parseAll=True)[0]
        except pyparsing.ParseException:
            return orig_string

    def parse(self, obj, **kwargs):
        """Recursively parse data structures."""
        # NOTE: Given that ConfigParser may be applied before this parsing and
        # ConfigParser only allows a single level of key/value pairs, nested
        # structures are stringified, and 'parse_string' does not recurses, so
        # we let the result data to be checked for instances of data structures
        # that need subsequent parsing.
        if isinstance(obj, str):
            obj = self.parse_string(obj, **kwargs)

        if isinstance(obj, dict):
            # NOTE: Keys are not modified
            obj = {k: self.parse(v, **kwargs) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple, set)):
            obj = type(obj)([self.parse(v, **kwargs) for v in obj])

        return obj

    def fix_configparser(self, obj: Any, **kwargs):
        """Recursively parse/fix data structures previously parsed by
        ConfigParser.

        Given that ConfigParser only allows a single level of
        key/value pairs, nested structures are stringified, so we apply regex
        transformations to conform multi-line values for 'parse_string()'.
        """
        if isinstance(obj, str):
            # Fix first nested element in mulitline values
            obj = re.sub(r'=\s*', '=', obj.strip())

            # Change nested elements in same level to be a CSV string
            obj = re.sub(r'\n', ',', obj.strip())

            # Quote interpolation strings
            obj = re.sub(r'["\']?(\${{[^}]+}})["\']?', r'"\1"', obj)

            obj = self.parse_string(obj, **kwargs)

            # Enable numeric protection for subsequent 'parse_string()'
            kwargs['protect_numerics'] = True

        if isinstance(obj, dict):
            obj = {
                k: self.fix_configparser(v, **kwargs)
                for k, v in obj.items()
            }
        elif isinstance(obj, (list, tuple, set)):
            obj = type(obj)([
                self.fix_configparser(v, **kwargs)
                for v in obj
            ])

        return obj

    def parse_with_configparser(self, config: Union[str, Dict[str, Any]]):
        """Parse with ConfigParser.

        Args:
            config (str, Dict[str, Any]): Configuration file/data to parse.
        """
        parser = configparser.ConfigParser(
            delimiters=('=',),
            allow_no_value=True,
        )

        # Leave options (keys) unchanged, do not lowercase
        parser.optionxform = lambda option: option

        # Expand environment variables
        config = self.expand_envvars(config)

        # Parse data
        if isinstance(config, str):
            if os.path.isfile(config):
                filename = config
                with open(filename) as fd:
                    config = fd.read()
                # NOTE: Expand environment variables from loaded data
                config = self.expand_envvars(config)

            parser.read_string(config)
        elif isinstance(config, dict):
            parser.read_dict(config)
        else:
            raise ValueError('invalid configuration data for ConfigParser')

        # Convert ConfigParser to dict (include defaults, if available)
        config = {
            section: dict(parser[section])
            for section in parser.sections()
        }
        if len(parser.defaults()) > 0:
            config[parser.default_section] = dict(parser.defaults())

        # Fix multi-line options
        return self.fix_configparser(config, protect_numerics=False)

    def load_from_string(self, data: str):
        """Parse a string into a dictionary.

        Returns:
            Dict[str:Any]: Configuration mapping
        """
        data = data.strip()
        try:
            config = json.loads(data)
        except json.decoder.JSONDecodeError:
            try:
                config = yaml.safe_load(data)
            except yaml.parser.ParserError:
                try:
                    config = self.parse_with_configparser(data)
                except Exception:
                    raise ValueError('invalid configuration string')
        return config

    def load_from_file(self, filename: str, file_type: str = None):
        """Load configuration data from YAML/JSON/INI file.

        Args:
            file_type (str): Explicit file format for file, ignore extension.

        Returns:
            dict[str:Any]: Configuration mapping
            None: If error/invalid occurs during parsing
        """
        filename = self.expand_envvars(filename)
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
            config = self.parse_with_configparser(filename)
        else:
            raise ValueError(f"invalid configuration file type, '{file_type}'")
        return config

    def load(
        self,
        config: Union[str, Dict[Any, Any]],
        *,
        keys: Union[str, Iterable[str]] = None,
        file_type: str = None,
        delimiter: str = ':',
    ):
        """Load/parse configuration from a file/string/mapping into a mapping.

        Note:
            * The transformations are idempotent with regards to the resulting
              configuration mapping.

        Args:
            config (str, Dict[Any, Any]): Configuration file/data to parse.
                File can embed keys via format: 'file.conf:key1:key2:...'
                Embedded keys supersede parameter keys. String formats can be
                dictionary-like or comma-delimited key=value pairs.

            keys (str, Iterable[str]): Extract only data from configuration
                corresponding to key. If multiple keys are provided, they
                are used (in order) to update the resulting configuration.

            file_type (str): Explicit file format for file, ignore extension.

            delimiter (str): Delimiter symbol between file name and keys.

        Returns:
            dict[str:Any]: Configuration mapping
        """
        if isinstance(config, str):
            config = self.expand_envvars(config)
            filename, *tmp_keys = config.split(delimiter)
            if os.path.isfile(filename):
                # Embedded keys supersede parameter keys
                if len(tmp_keys) > 0:
                    keys = tmp_keys
                config = self.load_from_file(filename, file_type=file_type)
                # If only a single configuration block, then use it
                if not keys and len(config) == 1:
                    keys = list(config.keys())

            # NOTE: Consider a string with no data structure related symbols
            # to be a filename. The symbols are based on the general parser.
            elif filename and not any(x in filename for x in PUNCTUATIONS):
                raise ValueError(
                    f"configuration file does not exists, '{filename}'"
                )
            else:
                config = self.load_from_string(config)

        elif isinstance(config, dict):
            config = self.expand_envvars(config)
        elif config is not None:
            raise ValueError(
                f"invalid configuration data type, '{type(config)}'"
            )

        if not config:
            print('Warning: no configuration detected', file=sys.stderr)
            return {}

        # Normalize strings and types based on a grammar
        config = self.parse(config)

        # Interpolate/substitute values
        config = self.interpolate(config)

        # Filter configuration based on keys
        if keys:
            if isinstance(keys, str):
                keys = [keys]

            filtered_config = {}
            for key in keys:
                filtered_config.update(config[key])
            config = filtered_config

        return config
