import os
import pandas
from typing import (
    Any,
    List,
    Dict,
    Union,
    Tuple,
    Callable,
    Iterable,
)


__all__ = [
    'iter_data',
    'valid_item',
    'unpack_dir',
    'is_iterable',
    'data_to_dict',
    'iterable_true',
    'corpus_generator',
    'valid_items_from_dict',
    'filter_indices_and_values',
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


def iter_data(
    data,
    keys,
    values,
    *,
    headers=None,
    valids=None,
    invalids=None,
    unique_keys=False,
    **kwargs
):
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

    Kwargs:
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

        unique_keys (bool): Control if keys can be repeated or not.

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
    if keys is None:
        keys = ()
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
    post_converters = None
    if key_value_check:
        post_converters = kwargs.get('converters')
        kwargs['converters'] = None

    # Pre-compute indices and columns for post-converter functions.
    # NOTE: This is a performance optimization because allows operating
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
        delimiter=kwargs.get('delimiter', '|'),
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
        reader = [reader]

    # Internal data structure for tracking unique keys.
    keys_processed = set()

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

            # Filter valid/invalild keys/values
            if key_value_check \
               and not valid_items_from_dict(usecols,
                                             kv,
                                             valids=valids,
                                             invalids=invalids):
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

            # Filter unique keys
            if unique_keys:
                if ks in keys_processed:
                    continue
                keys_processed.add(ks)

            # Organize values
            vs = (kv[num_keys] if num_values == 1
                  else tuple(kv[num_keys:values_stop]))

            yield ks, vs


def data_to_dict(
    *args,
    *,
    unique_values=False,
    **kwargs,
) -> Dict[Any, Union[Any, List[Any]]]:
    """Load paired data into a dictionary.

    Args (see 'iter_data')

    Kwargs (see 'iter_data'):
        unique_values (bool): Control if values can be repeated or not.
            Only applies if 'unique_keys' is False.

    Examples:
        >>> data = data_to_dict('MRSTY.RRF', ['cui'], ['sty'],
                                headers=HEADERS_MRSTY)
        >>> data = data_to_dict('MRSTY.RRF', [0], [1])
        >>> data = data_to_dict('MRSTY.RRF', ['cui'], ['sty'],
                                headers=HEADERS_MRSTY,
                                valids={'sty':ACCEPTED_SEMTYPES})
        >>> data = data_to_dict('MRSTY.RRF', ['cui'], ['sty', 'hier'],
                                headers=HEADERS_MRSTY)
        >>> data = data_to_dict('MRSTY.RRF', [0], [1, 2],
                                nrows=10, valids={1:None, 2:None})
    """
    unique_keys = kwargs.get('unique_keys', False)
    if unique_keys:
        # NOTE: Disable 'unique_keys' option for 'iter_data' because
        # dictionary already does that. To make them semantically
        # consistent, consider the firts appearance of a key. This is
        # because 'iter_data' considers the first appearance of a key
        # and dictionary updates consider the last appearance of a key.
        kwargs['unique_keys'] = False
        data = collections.defaultdict()
        for k, v in iter_data(*args, **kwargs):
            if k not in data:
                # Assume there is a single value per key.
                data[k] = v
    else:
        data = collections.defaultdict(list)
        for k, v in iter_data(*args, **kwargs):
            if not unique_values or v not in data[k]:
                data[k].append(v)
    return data


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
    corpora: Union[str, Iterable[str]],
    *,
    phony=False,
    **kwargs
) -> Tuple[str, str]:
    """Extracts text from corpora.

    Args:
        corpora (str): Text data to process. Valid values are:
            * Directory
            * File
            * Raw text
            * An iterable of any combination of the above

        phony (bool): If set, attr:`corpora` items are not considered
            as file system objects when name collisions occur.

    Kwargs:
        Options passed directory to 'unpack_dir'.

    Returns (Tuple[str, str]): Corpus source and corpus content.
                     The source identifier for raw text is '_text'.
                     The source identifier for other is their file system name.
    """
    if not is_iterable(corpora):
        corpora = (corpora,)

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
            yield '_text', corpus
        elif os.path.isfile(corpus):
            if os.path.basename(corpus).startswith('.'):
                continue
            # NOTE: For files, return the entire content.
            with open(corpus) as fd:
                yield corpus, fd.read()
