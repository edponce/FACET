import os
import pandas
import collections
from typing import (
    Any,
    Set,
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
        # invalids is not None
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

    An empty iterable or an iterable of Nones is considered false,
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
    **kwargs
) -> Iterator[Tuple[Any, Any]]:
    """Generator for data.

    Use Pandas 'read_csv()' to load data into a dataframe which is then
    iterated as key/value pairs.

    Args:
        data (str): File or buffer.
            See Pandas 'filepath_or_buffer' option from 'read_csv()'.

    Kwargs:
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
            row filtering (if 'valids' or 'invalids' are not provided, then
            this is used as Pandas 'converters' option from 'read_csv()').

    Kwargs (see Pandas 'read_csv()'):
        delimiter
        encoding
        converters
        skiprows
        nrows
        dtype
        iterator
        chunksize
        memory_map
        engine
    """
    if values is None:
        values = ()
    keys_values = (*keys, *values)

    # Get keys/values sizes into variables to prevent calling len()
    # for every iteration during processing.
    # NOTE: Performance-wise, not sure if this is worth it, but makes
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
    # NOTE: For large data sets, this may help improve performance because
    # when row filtering is enabled, converter functions are applied after
    # filtering occurs.
    key_value_check = iterable_true(valids) or iterable_true(invalids)

    # Check if converter functions need to be applied after row filtering.
    post_converters = kwargs.pop('converters', None)

    # Pre-compute indices and columns for post-converter functions.
    # NOTE: This is a performance optimization because it allows operating
    # on deterministic items without incurring on hashing operations
    # nor indirect addressing.
    if post_converters is not None:
        post_converters_idxcols = tuple(filter_indices_and_values(
                                        lambda x: x in post_converters,
                                        usecols))

    # Extend the column headers with a dummy header (hopefully unique).
    # NOTE: UMLS files end with a bar at each line and Pandas assumes
    # there is an extra column afterwards (only required if using the
    # 'python' engine).
    if headers is not None and kwargs.get('engine') == 'python':
        headers = list(headers) + [' ']

    # Data reader or iterator
    # NOTE: Could we benefit from using Modin.pandas? This function returns
    # a generator, but for the 'dict' version we could use DataFrames instead?
    reader = pandas.read_csv(
        data,
        names=headers,

        # Constant-ish settings
        delimiter=kwargs.get('delimiter', ','),
        usecols=usecols,
        header=None,
        index_col=False,
        na_filter=False,

        # Extra parameters
        encoding=kwargs.get('encoding', 'utf-8'),
        converters=kwargs.get('converters'),
        skiprows=kwargs.get('skiprows'),
        nrows=kwargs.get('nrows'),
        dtype=kwargs.get('dtype'),

        # Performance parameters
        iterator=kwargs.get('iterator', True),
        chunksize=kwargs.get('chunksize', 100000),
        memory_map=kwargs.get('memory_map', True),
        engine=kwargs.get('engine', 'c'),
    )

    # Place the dataframe into an iterable even if not iterating and
    # chunking through the data, so that it uses the same logic
    # as if it was a TextFileReader object.
    if not isinstance(reader, pandas.io.parsers.TextFileReader):
        reader = (reader,)

    # Iterate through dataframes or TextFileReader:
    #   a) Row filtering based on valid/invalid keys/values
    #   b) Apply post-converter functions
    #   c) Organize keys/values
    for df in reader:

        # Keys/values generator
        usecols_values = (df[col] for col in usecols)

        # NOTE: Uses lists instead of tuples so that individual items
        # can be modified with converter functions.
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

            # Apply post-converter functions
            if post_converters is not None:
                for idx, key in post_converters_idxcols:
                    if callable(post_converters[key]):
                        kv[idx] = post_converters[key](kv[idx])
                    else:
                        for f in post_converters[key]:
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

    Kwargs:
        Options passed directly to 'iload_data()'.
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
        if os.path.isdir(fd) and recursive:
            files.extend(unpack_dir(fd))
        elif os.path.isfile(fd):
            if not hidden and os.path.basename(fd).startswith('.'):
                continue
            files.append(fd)
    return files


def corpus_generator(
    corpora: Union[str, Iterable[str], Iterable[Iterable[str]]],
    *,
    phony=False,
    **kwargs,
) -> Tuple[str, str]:
    """Extracts text from corpora.

    Args:
        corpora (str): Text data to process. Valid values are:
            * Directory
            * File
            * Raw text
            * An iterable with two parts: (filename, text)
            * An iterable of any combination of the above

        phony (bool): If set, attr:`corpora` items are not considered
            as file system objects when name collisions occur.

    Kwargs:
        Options passed directory to 'unpack_dir'.

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
    # Filename and raw text format: (filename, text)
    # Directory format: (dir, None)

    if not phony:
        _corpora = []
        for corpus in corpora:
            if os.path.isdir(corpus):
                _corpora.extend(unpack_dir(corpus, **kwargs))
            else:
                _corpora.append(corpus)
    else:
        _corpora = corpora

    for corpus in _corpora:
        # Assume corpus is raw text if it is not a file system object.
        if phony or not os.path.exists(corpus):
            yield '__text__', corpus
        elif os.path.isfile(corpus):
            if os.path.basename(corpus).startswith('.'):
                continue
            # NOTE: For files, return the entire content. This is not the best
            # approach for large files, but there is no way of splitting by
            # sentences a priori (unless stated otherwise).
            with open(corpus) as fd:
                yield corpus, fd.read()


def get_obj_map_key(obj, class_map):
    """Resolve the key (or label) of an object via a key-class map."""
    for k, v in class_map.items():
        if isinstance(obj, v):
            return k
    return obj

