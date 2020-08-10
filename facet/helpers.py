import os
import pandas
import collections
import urllib.parse
from typing import (
    Any,
    List,
    Dict,
    Union,
    Tuple,
    Callable,
    Iterable,
    Iterator,
)


__all__ = [
    'load_data',
    'iload_data',
    'unpack_dir',
    'valid_item',
    'is_iterable',
    'iterable_true',
    'corpus_generator',
    'valid_items_from_dict',
    'filter_indices_and_values',
    'get_obj_map_key',
    'parse_address',
    'unparse_address',
    'parse_address_query',
    'unparse_address_query',
    'expand_envvars',
]


def valid_item(
    item: Any, *,
    valids: Iterable[Any] = None,
    invalids: Iterable[Any] = None,
) -> bool:
    """Detect if an item is valid based on given valid/invalid sequences.

    An item is valid if it is in the 'valids' sequence and not in the
    'invalids' sequence. A None sequence is not checked.

    Args:
        item (Any): Item to check for validity.

        valids (Iterable[Any]): Valid item values.

        invalids (Iterable[Any]): Invalid item values.
    """
    # NOTE: Check 'invalids' sequence last to prevent validity even if
    # item is in 'valids' sequence.
    is_valid = True
    if valids is not None:
        is_valid = item in valids
    if invalids is not None:
        is_valid = item not in invalids
    return is_valid


def valid_items_from_dict(
    keys: Iterable[Any],
    items: Iterable[Any],
    *,
    valids: Union[Dict[Any, Any], Iterable[Any]] = None,
    invalids: Union[Dict[Any, Any], Iterable[Any]] = None,
) -> bool:
    """
    Args:
        keys (Iterable[Any]): Keys corresponding to items.

        items (Iterable[Any]): Items to check for validity.

        valids (Dict[Any:Any|Iterable[Any]]): Valid keys/items.

        invalids (Dict[Any:Any|Iterable[Any]]): Invalid keys/items.
    """
    is_valid = False
    if valids is not None and invalids is not None:
        for k, item in zip(keys, items):
            if k in valids and k in invalids:
                if not valid_item(item,
                                  valids=valids[k],
                                  invalids=invalids[k]):
                    break
            elif k in valids:
                if not valid_item(item, valids=valids[k]):
                    break
            elif k in invalids:
                if not valid_item(item, invalids=invalids[k]):
                    break
        else:
            is_valid = True
    elif valids is not None:
        for k, item in zip(keys, items):
            if k in valids:
                if not valid_item(item, valids=valids[k]):
                    break
        else:
            is_valid = True
    else:
        # Case: invalids is not None
        for k, item in zip(keys, items):
            if k in invalids:
                if not valid_item(item, invalids=invalids[k]):
                    break
        else:
            is_valid = True
    return is_valid


def is_iterable(
    obj: Any,
) -> bool:
    return hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes))


def iterable_true(
    obj: Iterable[Any],
) -> bool:
    """Tests truth value for an iterable.

    An empty iterable or an iterable of None is considered false,
    otherwise, it is true.
    """
    return is_iterable(obj) and any(obj)


def filter_indices_and_values(
    predicate: Callable,
    iterable: Iterable[Any],
) -> Tuple[int, Any]:
    """Get index/value pairs for items in iterable that make predicate true.

    Args:
        predicate (Callable): Function to evaluate items in 'iterable'.

        iterable (Iterable[Any]): Sequence of items.

    Returns:
        (Tuple) Index/value tuples.
    """
    return ((idx, val) for idx, val in enumerate(iterable) if predicate(val))


def iload_data(
    data,
    *,
    keys: Iterable[Union[str, int]] = (0,),
    values: Iterable[Union[str, int]] = None,
    headers: Iterable[Union[str, int]] = None,
    valids: Dict[Union[str, int], Iterable[Any]] = None,
    invalids: Dict[Union[str, int], Iterable[Any]] = None,
    converters: Dict[Union[str, int], Iterable[Callable]] = None,
    **kwargs,
) -> Iterator[Tuple[Any, Any]]:
    """Generator for data.

    Use Pandas 'read_csv()' to load data into a dataframe which is then
    iterated as key/value pairs.

    Args:
        data (str): File or buffer.
            See Pandas 'filepath_or_buffer' option from 'read_csv()'.

        keys (Iterable[str|int]): Columns to use as dictionary keys.
            Multiple keys are stored as tuples in same order as given.
            If str, then it corresponds to 'headers' names.
            If int, then it corresponds to column indices.

        values (Iterable[str|int]): Columns to use as dictionary values.
            Multiple values are stored as tuples in same order as given.
            If str, then it corresponds to 'headers' names.
            If int, then it corresponds to column indices.

        headers (Iterable[str|int]): Column names.
            Headers are required when keys/values are str.
            Headers do not need to be complete, but do need to be in order
            and contain the keys/values identifier.

        valids (Dict[Any:Iterable[Any]]): Mapping between column identifiers
            and sequences of valid values to include.
            If values is None, then corresponding columns are included.

        invalids (Dict[Any:Iterable[Any]]): Mapping between column identifiers
            and sequences of invalid values to skip.
            Invalid values have precedence over valid values.
            If values is None, then corresponding columns are included.

        converters (Dict[Any:Callable|Iterable[Callable]]): Mapping between
            headers and (sequences of) converter functions to be applied after
            row filtering.

    Kwargs: Options forwarded to Pandas 'read_csv()', except for the
        following options which are ignored because they are set by
        internal decisions: filepath_or_buffer, names, usecols, header,
        index_col.
    """
    if values is None:
        values = ()
    keys_values = (*keys, *values)

    # Get keys/values sizes into variables to prevent calling len()
    # for every iteration during processing.
    # NOTE: Performance-wise, not sure if this has any effect, but makes
    # code cleaner.
    num_keys = len(keys)
    num_values = len(values)
    values_stop = len(keys_values)

    # Get columns used for filtering that are not part of keys/values
    filters = set()
    if valids is not None:
        filters.update(set(valids).difference(set(keys_values)))
    if invalids is not None:
        filters.update(set(invalids).difference(set(keys_values)))

    # Combine columns to use into a single data structure.
    usecols = (*keys_values, *filters)

    # Check if row filtering iterables for 'valid/invalid keys/values' are
    # provided. An empty iterable or an iterable of Nones is considered
    # as a no filtering request.
    # NOTE: Any iterable that supports the 'in' operator and has default
    # behavior using 'any()' is allowed.
    key_value_check = iterable_true(valids) or iterable_true(invalids)

    # Pre-compute indices and columns for converter functions.
    # NOTE: This is a performance optimization because it allows operating
    # on deterministic items without incurring on hashing operations
    # nor indirect addressing.
    if converters is not None:
        converters_idxcols = tuple(
            filter_indices_and_values(lambda x: x in converters, usecols)
        )

    # Extend the column headers with a dummy header (hopefully unique).
    # NOTE: UMLS files end with a bar at each line and Pandas assumes
    # there is an extra column afterwards (only required if using the
    # 'python' engine).
    if headers is not None and kwargs.get('engine') == 'python':
        headers = list(headers) + [' ']

    # Remove preset options for 'read_csv()'.
    for option in ('filepath_or_buffer', 'names', 'usecols', 'header',
                   'index_col'):
        if option in kwargs:
            del kwargs[option]
    # NOTE: 'delimiter' is an alias for 'sep' option.
    if 'delimiter' in kwargs:
        kwargs['sep'] = kwargs.pop('delimiter')

    # Data reader or iterator
    reader = pandas.read_csv(
        data,
        names=headers,

        # Constant-ish settings
        encoding=kwargs.pop('encoding', 'utf-8'),
        sep=kwargs.pop('sep', ','),
        usecols=usecols,
        header=None,
        index_col=False,
        na_filter=kwargs.pop('na_filter', False),

        # Performance parameters
        iterator=kwargs.pop('iterator', True),
        chunksize=kwargs.pop('chunksize', 10000),
        memory_map=kwargs.pop('memory_map', True),
        engine=kwargs.pop('engine', 'c'),

        # Other parameters
        **kwargs,
    )

    # Place the dataframe into an iterable even if not iterating and
    # chunking through the data, so that it uses the same logic
    # as if it was a TextFileReader object.
    if not isinstance(reader, pandas.io.parsers.TextFileReader):
        reader = (reader,)

    # Iterate through dataframes or TextFileReader:
    #   a) Row filtering based on valid/invalid keys/values
    #   b) Apply converter functions
    #   c) Organize keys/values
    for df in reader:

        # Keys/values generator
        usecols_values = (df[col] for col in usecols)

        # NOTE: Uses lists instead of tuples so that individual items
        # can be replaced after applying converter functions.
        for kv in map(list, zip(*usecols_values)):

            # Filter valid/invalid keys/values
            if (
                key_value_check
                and not valid_items_from_dict(
                    usecols,
                    kv,
                    valids=valids,
                    invalids=invalids,
                )
            ):
                continue

            # Apply converter functions
            if converters is not None:
                for idx, key in converters_idxcols:
                    if callable(converters[key]):
                        kv[idx] = converters[key](kv[idx])
                    else:
                        for f in converters[key]:
                            kv[idx] = f(kv[idx])

            # Organize keys
            ks = (kv[0] if num_keys == 1
                  else tuple(kv[:num_keys]))

            if num_values > 0:
                # Organize values
                vs = (kv[num_keys] if num_values == 1
                      else tuple(kv[num_keys:values_stop]))

                yield ks, vs
            else:
                yield ks


def load_data(
    data,
    *,
    keys: Iterable[Union[str, int]] = (0,),
    unique_keys: bool = False,
    multiple_values: bool = False,
    unique_values: bool = False,
    **kwargs,
) -> Union[List[Any], Dict[Any, Union[Any, List[Any]]]]:
    """Load data.

    If no values are provided, then return a list from keys.
    If values are provided, then return a dictionary of keys/values.

    Args:
        data (str): File or buffer.
            See Pandas 'filepath_or_buffer' option from 'read_csv()'.

    Kwargs:
        keys (Iterable[str|int]): Columns to use as dictionary keys.
            Multiple keys are stored as tuples in same order as given.
            If str, then it corresponds to 'headers' names.
            If int, then it corresponds to column indices.

        unique_keys (bool): Control if keys can be repeated or not.
            Only applies if 'values' is None.

        multiple_values (bool): Specify if values consist of single or multiple
            elements. For multi-value case, values are placed in an iterable
            container. For single-value case, the value is used as-is.
            Only applies if 'values' is not None.

        unique_values (bool): Control if values can be repeated or not.
            Only applies if 'multiple_values' is True.

    Kwargs: Options forwarded to 'iload_data()'.
    """
    if kwargs.get('values') is None:
        if unique_keys:
            # NOTE: Convert to a list because JSON does not serializes sets.
            _data = list(set(iload_data(data, keys=keys, **kwargs)))
        else:
            _data = list(iload_data(data, keys=keys, **kwargs))
    elif multiple_values:
        if unique_values:
            _data = collections.defaultdict(list)
            for k, v in iload_data(data, keys=keys, **kwargs):
                if v not in _data[k]:
                    _data[k].append(v)
        else:
            _data = collections.defaultdict(list)
            for k, v in iload_data(data, keys=keys, **kwargs):
                _data[k].append(v)
    else:
        # Consider the value of the first appearance of a key.
        _data = {}
        for k, v in iload_data(data, keys=keys, **kwargs):
            if k not in _data:
                _data[k] = v
    return _data


def unpack_dir(
    adir: str,
    *,
    hidden: bool = False,
    recursive: bool = False,
) -> List[str]:
    """Unpack directories into a list of filenames.

    Args:
        adir (str): Directory name.

        hidden (bool): If set, include hidden files.

        recursive (bool): If set, unpack files recursively.

    Returns (List[str]): List of filenames.
    """
    # NOTE: Probably better to use os.walk().
    files = []
    for file_or_dir in os.listdir(adir):
        fd = os.path.join(adir, file_or_dir)
        if recursive and os.path.isdir(fd):
            files.extend(unpack_dir(fd))
        elif os.path.isfile(fd):
            if not hidden and os.path.basename(fd).startswith('.'):
                continue
            files.append(fd)
    return files


def expand_envvars(string: str):
    """Expand user and environment variables."""
    return os.path.expandvars(os.path.expanduser(string))


def corpus_generator(
    corpora: Union[str, Iterable[str], Iterable[Iterable[str]]],
    *,
    source_only: bool = False,
    phony: bool = False,
    recursive: bool = False,
) -> Tuple[str, str]:
    """Extracts text from corpora.

    Args:
        corpora (str): Text data to process. Valid values are:
            * Directory
            * File
            * Raw text
            * An iterable with two parts: (filename_or_id, text)
            * An iterable of any combination of the above

        source_only (bool): If set, only return the sources (file names).
            Raw text is returned as is.

        phony (bool): If set, attr:`corpora` items are not considered
            as file system objects when name collisions occur.

        recursive (bool): If set, unpack files recursively.

    Returns (Tuple[str, str]): Corpus source and corpus content.
                     The source identifier for raw text is '__text__'.
                     The source identifier for other is their file system name.
    """
    if not is_iterable(corpora):
        corpora = (corpora,)

    # NOTE: Need to implement the following scheme.
    # Convert corpus into iterable of iterables [(filename1, text), (...), ...]
    # File format: (filename, None)
    # Raw text format: (None, text)
    # Filename and raw text format: (filename_or_id, text)
    # Directory format: (dir, None)

    if not phony:
        _corpora = []
        for corpus in corpora:
            corpus = expand_envvars(corpus)
            if os.path.isdir(corpus):
                _corpora.extend(unpack_dir(corpus, recursive=recursive))
            else:
                _corpora.append(corpus)
    else:
        _corpora = corpora

    for corpus in _corpora:
        corpus = expand_envvars(corpus)
        # Assume corpus is raw text if it is not a file system object.
        if phony or not os.path.exists(corpus):
            yield corpus if source_only else ('__text__', corpus)
        elif os.path.isfile(corpus):
            if os.path.basename(corpus).startswith('.'):
                continue
            # NOTE: For files, return the entire content. This is not the best
            # approach for large files, but there is no way of splitting by
            # sentences a priori (unless stated otherwise).
            with open(corpus) as fd:
                yield corpus if source_only else (corpus, fd.read())


def get_obj_map_key(obj, class_map):
    """Resolve the key (or label) of an object via a key-class map."""
    for k, v in class_map.items():
        if isinstance(obj, v):
            return k
    return obj


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
