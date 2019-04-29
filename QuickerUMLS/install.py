import os
import sys
import time
import pandas
import shutil
import argparse
import collections
from toolbox import CuiSemTypesDB, SimstringDBWriter
from constants import HEADERS_MRCONSO, HEADERS_MRSTY, LANGUAGES, ACCEPTED_SEMTYPES


try:
    from unidecode import unidecode
    UNIDECODE_AVAIL = True
except ImportError:
    UNIDECODE_AVAIL = False

# 0 = No status info
# 1 = General status info
# 2 = Detailed status info
PROFILE = 2


# NOTE: UMLS headers should be automatically parsed from UMLS MRFILES.RRF.

########################################################################
# Converter functions
########################################################################
def lowercase_str(term):
    return term.lower()


def normalize_unicode_str(term):
    return unidecode(term)


def is_preferred(ispref):
    return 1 if ispref == 'Y' else 0
########################################################################


def valid_item(item, *,
               valids=None,
               invalids=None):
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


def valid_items_from_seq(items, *,
                         valids=None,
                         invalids=None):
    """
    Args:
        items (Iterable[Any]): Items to check for validity.

        valids (Iterable[Any|Iterable[Any]]): Sequences with corresponding
            valid item values.

        invalids (Iterable[Any|Iterable[Any]]): Sequences with corresponding
            invalid item values.
    """
    is_valid = False
    if valids is not None and invalids is not None:
        for item, valid_seq, invalid_seq in zip(items, valids, invalids):
            if not valid_item(item, valids=valid_seq, invalids=invalid_seq):
                break
        else:
            is_valid = True
    elif valids is not None:
        for item, valid_seq in zip(items, valids):
            if not valid_item(item, valids=valid_seq):
                break
        else:
            is_valid = True
    else:
        for item, invalid_seq in zip(items, invalids):
            if not valid_item(item, invalids=invalid_seq):
                break
        else:
            is_valid = True
    return is_valid


def valid_items_from_dict(keys,
                          items, *,
                          valids=None,
                          invalids=None):
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
                if not valid_item(item, valids=valids[k], invalids=invalids[k]):
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


def iterable_true(iterable):
    """Tests truth value for an iterable.

    An empty iterable or an iterable of Nones is considered false,
    otherwise, it is true.
    """
    return isinstance(iterable, (list, set, tuple, dict)) and any(iterable)


def filter_indices_and_values(predicate, iterable):
    """Get index/value pairs for items in iterable that make predicate true.

    Args:
        predicate (Callable): Function to evaluate items in 'iterable'.

        iterable (Iterable): Sequence of items.

    Returns:
        Generator of index/value tuples.
    """
    return ((idx, val) for idx, val in enumerate(iterable) if predicate(val))


def iter_umls(rrf_data,
              keys,
              values, *,
              headers=None,
              valids=None,
              invalids=None,
              multi_converters=None,
              **kwargs):
    """Generator for UMLS data.

    Use Pandas 'read_csv()' to load data into a dataframe which is then
    iterated as tuples of keys/values.

    Args:
        rrf_data (str): Path of UMLS RRF file or buffer.

        keys (Iterable[str|int]): Columns to use as dictionary keys.
            Multiple keys are stored as tuples in same order as given.
            If str, then it corresponds to a 'headers' names.
            If int, then it corresponds to a column index.

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

        multi_converters (Dict[Any:Iterable[Callable]]): Mapping between
            headers and sequences of converter functions to be applied after
            row filtering (and after regular/post-converter functions).

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

    Examples:
        >>> umls = umls_to_dict('MRSTY.RRF', 'cui', 'sty', headers=HEADERS_MRSTY)
        >>> umls = umls_to_dict('MRSTY.RRF', 0, 1)
        >>> umls = umls_to_dict('MRSTY.RRF', 'cui', 'sty', headers=HEADERS_MRSTY, valid_values=ACCEPTED_SEMTYPES)
        >>> umls = umls_to_dict('MRSTY.RRF', 'cui', ['sty', 'hier'], headers=HEADERS_MRSTY)
        >>> umls = umls_to_dict('MRSTY.RRF', 0, [1, 2], nrows=10, valid_values=[None, None])
    """
    # HACK: Extend the column headers with a dummy header (hopefully unique).
    # NOTE: UMLS files end with a bar at each line and Pandas assumes
    # there is an extra column afterwards (only required if using the
    # 'python' engine).
    if headers is not None and kwargs.get('engine') == 'python':
        headers = tuple(headers) + ('_',)

    # Get keys/values sizes into variables to prevent calling len()
    # for every iteration during processing.
    # NOTE: Performance-wise, not sure if this is worth it, but makes
    # code cleaner.
    num_keys = len(keys)
    num_values = len(values)

    # Combine columns to use into single data structure.
    usecols = (*keys, *values)

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

    # Pre-compute indices and columns for post/multi-converter functions.
    # NOTE: This is a performance optimization because allows operating
    # on deterministic items without incurring on hashing operations
    # nor indirect addressing.
    if post_converters is not None:
        post_converters_idxcols = tuple(filter_indices_and_values(
                                        lambda x: x in post_converters,
                                        usecols))
    if multi_converters is not None:
        multi_converters_idxcols = tuple(filter_indices_and_values(
                                         lambda x: x in multi_converters,
                                         usecols))

    # Data reader or iterator
    reader = pandas.read_csv(rrf_data,
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
                             engine=kwargs.get('engine', 'c'))

    # Place the dataframe into an iterable even if not iterating and
    # chunking through the data, so that it uses the same logic
    # as if it was a TextFileReader object.
    if not isinstance(reader, pandas.io.parsers.TextFileReader):
        reader = (reader,)

    # Iterate through dataframes or TextFileReader:
    #   a) Row filtering based on valid/invalid keys/values
    #   b) Apply post/multi-converter functions
    #   c) Organize keys/values
    for df in reader:
        # Keys/values generator
        keys_values = (df[col] for col in usecols)

        # NOTE: Uses lists instead of tuples so that individual items
        # can be modified with converter functions.
        for kv in map(list, zip(*keys_values)):

            # Filter valid/invalild keys/values
            if key_value_check \
               and not valid_items_from_dict(usecols,
                                             kv,
                                             valids=valids,
                                             invalids=invalids):
                continue

            # Apply post-converter functions
            # NOTE: Post-converter functions occur before multi-converter
            # functions to follow same logic as if post-converter functions
            # had been applied by 'read_csv()' while reading data.
            if post_converters is not None:
                for idx, key in post_converters_idxcols:
                    kv[idx] = post_converters[key](kv[idx])

            # Apply multi-converter functions
            if multi_converters is not None:
                for idx, key in multi_converters_idxcols:
                    for f in multi_converters[key]:
                        kv[idx] = f(kv[idx])

            # Organize keys/values
            ks = kv[0] if num_keys == 1 else tuple(kv[:num_keys])
            vs = kv[num_keys] if num_values == 1 else tuple(kv[num_keys:])

            yield ks, vs


def iter_umls0(rrf_data,
              keys,
              values, *,
              headers=None,
              valid_keys=None,
              valid_values=None,
              invalid_keys=None,
              invalid_values=None,
              multi_converters=None,
              **kwargs):
    """Generator for UMLS data.

    Use Pandas 'read_csv()' to load data into a dataframe which is then
    iterated as tuples of keys/values.

    Args:
        rrf_data (str): Path of UMLS RRF file or buffer.

        keys (Iterable[str|int]): Columns to use as dictionary keys.
            Multiple keys are stored as tuples in same order as given.
            If str, then it corresponds to a 'headers' names.
            If int, then it corresponds to a column index.

        values (Iterable[str|int]): Columns to use as dictionary values.
            Multiple values are stored as tuples in same order as given.
            If str, then it corresponds to 'headers' names.
            If int, then it corresponds to column indices.

    Kwargs:
        headers (Iterable[str|int]): Column names.
            Headers are required when keys/values are str.
            Headers do not need to be complete, but do need to be in order
            and contain the keys/values identifier.

        valid_keys (Iterable[None|Iterable[Any]]): Valid keys to include.
            If None, then all keys are included.

        valid_values (Iterable[None|Iterable[Any]]): Valid values to include.
            If None, then all values are included.

        invalid_keys (Iterable[None|Iterable[Any]]): Invalid keys to skip.
            Invalid keys have precedence over valid keys.
            If None, then all keys are included.

        invalid_values (Iterable[None|Iterable[Any]]): Invalid values to skip.
            Invalid values have precedence over valid values.
            If None, then all values are included.

        multi_converters (Dict[Any:Iterable[Callable]]): Mapping between
            headers and sequences of converter functions to be applied after
            row filtering (and after regular/post-converter functions).

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

    Examples:
        >>> umls = umls_to_dict('MRSTY.RRF', 'cui', 'sty', headers=HEADERS_MRSTY)
        >>> umls = umls_to_dict('MRSTY.RRF', 0, 1)
        >>> umls = umls_to_dict('MRSTY.RRF', 'cui', 'sty', headers=HEADERS_MRSTY, valid_values=ACCEPTED_SEMTYPES)
        >>> umls = umls_to_dict('MRSTY.RRF', 'cui', ['sty', 'hier'], headers=HEADERS_MRSTY)
        >>> umls = umls_to_dict('MRSTY.RRF', 0, [1, 2], nrows=10, valid_values=[None, None])
    """
    # HACK: Extend the column headers with a dummy header (hopefully unique).
    # NOTE: UMLS files end with a bar at each line and Pandas assumes
    # there is an extra column afterwards (only required if using the
    # 'python' engine).
    # if headers is not None and kwargs.get('engine') == 'python':
    if headers is not None:
        headers = tuple(headers) + ('_',)

    # Get keys/values sizes into variables to prevent calling len()
    # for every iteration during processing.
    # NOTE: Performance-wise, not sure if this is worth it, but makes
    # code cleaner.
    num_keys = len(keys)
    num_values = len(values)

    # Combine columns to use into single data structure.
    usecols = (*keys, *values)

    # Check if row filtering iterables for 'valid/invalid keys/values' are
    # provided. An empty iterable or an iterable of Nones is considered
    # as a no filtering request.
    # NOTE: Any iterable that supports the 'in' operator and has default
    # behavior using 'any()' is allowed.
    # NOTE: For large data sets, this may help improve performance because
    # when row filtering is enabled, converter functions are applied after
    # filtering occurs.
    key_check = iterable_true(valid_keys) or iterable_true(invalid_keys)
    value_check = iterable_true(valid_values) or iterable_true(invalid_values)

    # Check if converter functions need to be applied after row filtering.
    post_converters = None
    if key_check or value_check:
        post_converters = kwargs.get('converters')
        kwargs['converters'] = None

    # Pre-compute indices and columns for post/multi-converter functions.
    # NOTE: This is a performance optimization because allows operating
    # on deterministic items without incurring on hashing operations
    # nor indirect addressing.
    if post_converters is not None:
        post_converters_idxcols = tuple((idx, col)
                                        for idx, col in enumerate(usecols)
                                        if col in post_converters)
    if multi_converters is not None:
        multi_converters_idxcols = tuple((idx, col)
                                         for idx, col in enumerate(usecols)
                                         if col in multi_converters)

    # Data reader or iterator
    reader = pandas.read_csv(rrf_data,
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
                             engine=kwargs.get('engine', 'c'))

    # Place the dataframe into an iterable even if not iterating and
    # chunking through the data, so that it uses the same logic
    # as if it was a TextFileReader object.
    if not isinstance(reader, pandas.io.parsers.TextFileReader):
        reader = (reader,)

    # Iterate through dataframes or TextFileReader:
    #   a) Row filtering based on valid/invalid keys/values
    #   b) Apply post/multi-converter functions
    #   c) Organize keys/values
    for df in reader:
        # Keys/values generators
        keys_values = (df[col] for col in usecols)

        # NOTE: Uses lists instead of tuples so that individual items
        # can be modified with converter functions.
        for kv in map(list, zip(*keys_values)):

            # Filter invalild keys
            if key_check \
               and not valid_items_from_seq(kv[:num_keys],
                                            valids=valid_keys,
                                            invalids=invalid_keys):
                continue

            # Filter invalild values
            if value_check \
               and not valid_items_from_seq(kv[num_keys:],
                                            valids=valid_values,
                                            invalids=invalid_values):
                continue

            # Apply post-converter functions
            # NOTE: Post-converter functions occur before multi-converter
            # functions to follow same logic as if post-converter functions
            # had been applied by 'read_csv()' while reading data.
            if post_converters is not None:
                for idx, key in post_converters_idxcols:
                    kv[idx] = post_converters[key](kv[idx])

            # Apply multi-converter functions
            if multi_converters is not None:
                for idx, key in multi_converters_idxcols:
                    for f in multi_converters[key]:
                        kv[idx] = f(kv[idx])

            # Organize keys/values
            ks = kv[0] if num_keys == 1 else tuple(kv[:num_keys])
            vs = kv[num_keys] if num_values == 1 else tuple(kv[num_keys:])

            yield ks, vs


def umls_to_dict(*args, **kwargs):
    """Load UMLS data into a dictionary.

    Args (see 'iter_umls)

    Kwargs (see 'iter_umls')
    """
    umls = collections.defaultdict(set)
    for k, v in iter_umls(*args, **kwargs):
        umls[k].add(v)
    return umls


def extract_mrconso(mrconso_file, cuisty, mrconso_header=HEADERS_MRCONSO,
                    lowercase=False,
                    normalize_unicode=False,
                    language=['ENG']):
    with open(mrconso_file, 'r') as f:
        # Parse lines only until the fields we need, ignore remaining
        str_idx = mrconso_header.index('str')
        cui_idx = mrconso_header.index('cui')
        pref_idx = mrconso_header.index('ispref')
        lat_idx = mrconso_header.index('lat')
        max_split = max(str_idx, cui_idx, pref_idx, lat_idx) + 1

        processed = set()

        # Profile
        num_valid_lang = 0
        num_repeated_cuiterm = 0
        num_lines = 0

        for ln in f:
            # Profile
            num_lines += 1

            content = ln.strip().split('|', max_split)

            if content[lat_idx] not in language:
                continue

            # Profile
            num_valid_lang += 1

            cui = content[cui_idx]
            term = content[str_idx].strip()

            if (cui, term) in processed:
                # Profile
                num_repeated_cuiterm += 1
                continue

            processed.add((cui, term))

            if lowercase:
                term = term.lower()

            if normalize_unicode:
                term = unidecode(term)

            preferred = 1 if content[pref_idx] else 0

            yield (term, cui, cuisty[cui], preferred)

        # Profile
        if PROFILE > 1:
            print(f'Num lines to process: {num_lines}')
            print(f'Num valid language: {num_valid_lang}')
            print(f'Num repeated CUI-term: {num_repeated_cuiterm}')
            print(f'Num processed: {len(processed)}')
            print(f'Size of processed CUI-term: {sys.getsizeof(processed)}')


def dump_conso_sty(extracted_it, cuisty_dir,
                   bulk_size=1000, status_step=100000):
    # Profile
    prev_time = time.time()
    num_terms = 0

    cuisty_db = CuiSemTypesDB(cuisty_dir)
    terms = set()
    cui_bulk = []
    sty_bulk = []
    for i, (term, cui, stys, preferred) in enumerate(extracted_it, start=1):
        # Profile
        num_terms += 1

        terms.add(term)

        if len(cui_bulk) == bulk_size:
            cuisty_db.bulk_insert_cui(cui_bulk)
            cuisty_db.bulk_insert_sty(sty_bulk)
            cui_bulk = []
            sty_bulk = []
        else:
            cui_bulk.append((term, cui, preferred))
            sty_bulk.append((cui, stys))

        # Profile
        if PROFILE > 1 and i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')
            prev_time = curr_time

    # Flush remaining ones
    if len(cui_bulk) > 0:
        cuisty_db.bulk_insert_cui(cui_bulk)
        cuisty_db.bulk_insert_sty(sty_bulk)
        cui_bulk = []
        sty_bulk = []

        # Profile
        if PROFILE > 1:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')

    # Profile
    if PROFILE > 0:
        print(f'Num terms: {num_terms}')
        print(f'Num unique terms: {len(terms)}')
        print(f'Size of Simstring terms: {sys.getsizeof(terms)}')

    return terms


def dump_terms(simstring_dir, simstring_terms,
               status_step=100000):
    # Profile
    prev_time = time.time()

    ss_db = SimstringDBWriter(simstring_dir)
    for i, term in enumerate(simstring_terms, start=1):
        ss_db.insert(term)

        # Profile
        if PROFILE > 1 and i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')
            prev_time = curr_time


def driver(opts):
    # UMLS files
    mrconso_file = os.path.join(opts.umls_dir, 'MRCONSO.RRF')
    mrsty_file = os.path.join(opts.umls_dir, 'MRSTY.RRF')

    # Create install directories for the two databases
    simstring_dir = os.path.join(opts.install_dir, 'umls-simstring.db')
    cuisty_dir = os.path.join(opts.install_dir, 'cui-semtypes.db')
    os.makedirs(simstring_dir)
    os.makedirs(cuisty_dir)

    converters = {'sty': [lowercase_str, normalize_unicode_str]}
    valids = {'sty': ACCEPTED_SEMTYPES}
    invalids = {'cui': ['C0000005']}

    print('Loading semantic types...')
    start = time.time()
    cuisty = umls_to_dict(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY)
    # cuisty = umls_to_dict(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY, valids=valids)
    # cuisty = umls_to_dict(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY, valids=valids, invalids=invalids)
    # cuisty = umls_to_dict(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY, multi_converters=converters)
    curr_time = time.time()
    print(f'Loading semantic types: {curr_time - start} s')

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    sys.exit()

    # Set converter functions
    converters = {}
    if opts.lowercase and opts.normalize_unicode:
        converters['str'] = lowercase_and_normalize_unicode_str
    elif opts.lowercase:
        converters['str'] = lowercase_str
    else:
        converters['str'] = normalize_unicode_str
    converters['ispref'] = is_preferred

    print('Loading concepts...')
    start = time.time()
    conso = umls_to_dict(mrconso_file, 'str', ('cui', 'lat', 'ispref'), headers=HEADERS_MRCONSO, nrows=20, valid_values=(cuisty.keys(), {'ENG'}, None), converters=converters)
    # conso = umls_to_dict(mrconso_file, 'str', ('cui', 'lat', 'ispref'), headers=HEADERS_MRCONSO, nrows=20, valid_values=(None, {'ENG'}, None), converters=converters)
    curr_time = time.time()
    print(f'Loading concepts: {curr_time - start} s')

    # Profile
    if PROFILE > 1:
        print(conso)
        print(f'Num unique Terms: {len(conso)}')
        print(f'Num values in Term-CUI/Lat/IsPref dictionary: {sum(len(v) for v in conso.values())}')
        print(f'Size of Term-CUI/Lat/IsPref dictionary: {sys.getsizeof(conso)}')

    sys.exit()

    print('Loading and parsing concepts...')
    start = time.time()
    conso_cuisty_iter = extract_mrconso(
                            mrconso_file, cuisty,
                            lowercase=opts.lowercase,
                            normalize_unicode=opts.normalize_unicode,
                            language=opts.language)
    terms = dump_conso_sty(conso_cuisty_iter, cuisty_dir)
    curr_time = time.time()
    print(f'Loading and parsing concepts: {curr_time - start} s')

    print('Writing Simstring database...')
    start = time.time()
    dump_terms(simstring_dir, terms)
    curr_time = time.time()
    print(f'Writing Simstring database: {curr_time - start} s')


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument(
        '-u', '--umls_dir', required=True,
        help='Directory of UMLS RRF files'
    )
    args.add_argument(
        '-i', '--install_dir', required=True,
        help='Directory for installing QuickerUMLS files'
    )
    args.add_argument(
        '-l', '--lowercase', action='store_true',
        help='Consider only lowercase version of tokens'
    )
    args.add_argument(
        '-n', '--normalize-unicode', action='store_true',
        help='Normalize unicode strings to their closest ASCII representation'
    )
    args.add_argument(
        '-e', '--language', default={'ENG'}, action='append',
        choices={k for k, v in LANGUAGES.items() if v is not None},
        help='Extract concepts of the specified language'
    )
    clargs = args.parse_args()

    if not os.path.exists(clargs.install_dir):
        print(f'Creating install directory: {clargs.install_dir}')
        os.makedirs(clargs.install_dir)
    elif len(os.listdir(clargs.install_dir)) > 0:
        print(f'Install directory ({clargs.install_dir}) is not empty...aborting')
        shutil.rmtree(clargs.install_dir)
        os.mkdir(clargs.install_dir)
        # exit(1)

    if clargs.normalize_unicode:
        if not UNIDECODE_AVAIL:
            err = ("""'unidecode' is needed for unicode normalization
                   please install it via the 'pip install unidecode'
                   command.""")
            print(err, file=sys.stderr)
            exit(1)
        flag_fp = os.path.join(clargs.install_dir, 'normalize-unicode.flag')
        open(flag_fp, 'w').close()

    if clargs.lowercase:
        flag_fp = os.path.join(clargs.install_dir, 'lowercase.flag')
        open(flag_fp, 'w').close()

    flag_fp = os.path.join(clargs.install_dir, 'language.flag')
    with open(flag_fp, 'w') as f:
        f.write(os.linesep.join(clargs.language))

    start = time.time()
    driver(clargs)
    curr_time = time.time()
    print(f'Total runtime: {curr_time - start} s')
