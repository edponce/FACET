import os
import sys
import time
import pandas
import shutil
import argparse
import collections
from unidecode import unidecode
from .toolbox import (CuiSemTypesDB,
                      SimstringDBWriter)
from .constants import (HEADERS_MRCONSO,
                        HEADERS_MRSTY,
                        LANGUAGES,
                        ACCEPTED_SEMTYPES)

# 0 = No status info
# 1 = Processing rate
# 2 = Data structures info
VERBOSE = 1
nrows = None


########################################################################
# Internal Configuration
########################################################################
CONCEPTS_FILE = 'MRCONSO.RRF'
SEMANTIC_TYPES_FILE = 'MRSTY.RRF'
SIMSTRING_DB = 'umls-simstring.db'
LEVEL_DB = 'cui-semtypes.db'


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


def is_iterable(obj):
    return hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes))


def iterable_true(obj):
    """Tests truth value for an iterable.

    An empty iterable or an iterable of Nones is considered false,
    otherwise, it is true.
    """
    return is_iterable(obj) and any(obj)


def filter_indices_and_values(predicate, iterable):
    """Get index/value pairs for items in iterable that make predicate true.

    Args:
        predicate (Callable): Function to evaluate items in 'iterable'.

        iterable (Iterable): Sequence of items.

    Returns:
        Generator of index/value tuples.
    """
    return ((idx, val) for idx, val in enumerate(iterable) if predicate(val))


def iter_data(data,
              keys,
              values, *,
              headers=None,
              valids=None,
              invalids=None,
              unique_keys=False,
              **kwargs):
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
    reader = pandas.read_csv(data,
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
            # NOTE: Can't we simply use kv[num_keys:]?
            vs = (kv[num_keys] if num_values == 1
                  else tuple(kv[num_keys:values_stop]))
                  # else tuple(kv[num_keys:]))

            yield ks, vs


def data_to_dict(*args, **kwargs):
    """Load paired data into a dictionary.

    Args (see 'iter_data')

    Kwargs (see 'iter_data')

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
        # Disable 'unique_keys' option for 'iter_data' because
        # dictionary already does that.
        kwargs['unique_keys'] = False
        data = collections.defaultdict()
        for k, v in iter_data(*args, **kwargs):
            # Assume there is a single value per key
            if k not in data:
                data[k] = v
    else:
        data = collections.defaultdict(list)
        for k, v in iter_data(*args, **kwargs):
            # Do not duplicate values for a key
            if k not in data:
                data[k].append(v)
            elif v not in data[k]:
                data[k].append(v)
    return data


# NOTE: Stores CUI-Semantic Type mapping
# cui: [sty, ...]
def dump_cuisty(data,
                db_dir,
                bulk_size=1000,
                status_step=100000):
    # Profile
    prev_time = time.time()

    # Database connection
    # db = CuiSemTypesDB(db_dir)

    # NOTE: Do we need really to extract items?
    if isinstance(data, dict):
        data = data.items()

    with UMLSDB(db_dir) as db:
        bulk_data = []
        for i, cui_sty in enumerate(data, start=1):
            if len(bulk_data) == bulk_size:
                db.bulk_insert_sty(bulk_data)
                bulk_data = []
            else:
                bulk_data.append(cui_sty)

            if VERBOSE > 0 and i % status_step == 0:
                curr_time = time.time()
                print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / (bulk_size * (status_step / bulk_size))} s/batch')
                prev_time = curr_time

        # Flush remaining ones
        if len(bulk_data) > 0:
            db.bulk_insert_sty(bulk_data)

            if VERBOSE > 0:
                curr_time = time.time()
                print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / len(bulk_data)} s/batch')


# NOTE: Stores Term-CUI,Preferred mapping
# term: [(CUI,pref), ...]
def dump_conso(data,
               db_dir,
               bulk_size=1000,
               status_step=100000):
    # Profile
    prev_time = time.time()

    # Database connection
    # cuisty_db = CuiSemTypesDB(db_dir)

    # NOTE: Do we need really to extract items?
    if isinstance(conso, dict):
        data = data.items()

    with UMLSDB(db_dir) as db:
        terms = set()
        bulk_data = []
        for i, ((term, cui), preferred) in enumerate(data, start=1):
            terms.add(term)

            if len(bulk_data) == bulk_size:
                db.safe_bulk_insert_cui(bulk_data)
                bulk_data = []
            else:
                bulk_data.append((term, cui, preferred))

            if VERBOSE > 0 and i % status_step == 0:
                curr_time = time.time()
                print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / (bulk_size * (status_step / bulk_size))} s/batch')
                prev_time = curr_time

        # Flush remaining ones
        if len(bulk_data) > 0:
            db.safe_bulk_insert_cui(bulk_data)

            if VERBOSE > 0:
                curr_time = time.time()
                print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / len(bulk_data)} s/batch')

    if VERBOSE > 1:
        print(f'Num terms: {i}')
        print(f'Num unique terms: {len(terms)}')
        print(f'Size of Simstring terms: {sys.getsizeof(terms)}')

    return terms


def dump_terms(terms,
               db_dir,
               bulk_size=1,
               status_step=100000):
    prev_time = time.time()
    with SimstringDBWriter(db_dir) as db:
        for i, term in enumerate(terms, start=1):
            db.write(term)
            if VERBOSE > 0 and i % status_step == 0:
                curr_time = time.time()
                print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / (bulk_size * (status_step / bulk_size))} s/batch')
                prev_time = curr_time


def driver(opts):
    # UMLS files
    mrconso_file = os.path.join(opts.umls_dir, CONCEPTS_FILE)
    mrsty_file = os.path.join(opts.umls_dir, SEMANTIC_TYPES_FILE)

    # Create install directory
    if not os.path.exists(opts.install_dir) or not os.path.isdir(opts.install_dir):
        os.makedirs(opts.install_dir)

    # Set converter functions
    converters = collections.defaultdict(list)
    if opts.lowercase:
        converters['str'].append(lowercase_str)
    if opts.normalize_unicode:
        converters['str'].append(normalize_unicode_str)
    converters['ispref'].append(is_preferred)

    print('Loading/parsing concepts...')
    start = time.time()
    conso = iter_data(mrconso_file, ['str', 'cui'], ['ispref'], headers=HEADERS_MRCONSO, valids={'lat': opts.language}, converters=converters, unique_keys=True, nrows=nrows)

    # NOTE: This version is the same as above with the only difference
    # that it returns a dictionary data structure which is necessary
    # if semantic types are going to use it as a filter for CUIs.
    # See NOTE in loading of semantic types.
    # conso = data_to_dict(mrconso_file, ['str', 'cui'], ['ispref'], headers=HEADERS_MRCONSO, valids={'lat': opts.language}, converters=converters, nrows=nrows)
    curr_time = time.time()
    print(f'Loading/parsing concepts: {curr_time - start} s')

    print('Writing concepts...')
    start = time.time()
    cuisty_dir = os.path.join(opts.install_dir, LEVEL_DB)
    terms = dump_conso(conso, cuisty_dir)
    curr_time = time.time()
    print(f'Writing concepts: {curr_time - start} s')

    print('Writing Simstring database...')
    start = time.time()
    simstring_dir = os.path.join(opts.install_dir, SIMSTRING_DB)
    dump_terms(terms, simstring_dir)
    curr_time = time.time()
    print(f'Writing Simstring database: {curr_time - start} s')

    print('Loading/parsing semantic types...')
    start = time.time()
    cuisty = iter_data(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY, valids={'sty': ACCEPTED_SEMTYPES}, nrows=nrows)

    # NOTE: These versions only considers CUIs that are found in the
    # concepts ('conso') data structure. It requires that the 'conso'
    # variable is in dictionary form. Although this approach reduces the
    # installation time, it reduces the database size.
    # cuisty = iter_data(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY, valids={'cui': {k[1] for k in conso.keys()}, 'sty': ACCEPTED_SEMTYPES}, nrows=nrows)
    # cuisty = data_to_dict(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY, valids={'cui': {k[1] for k in conso.keys()}, 'sty': ACCEPTED_SEMTYPES}, nrows=nrows)
    curr_time = time.time()
    print(f'Loading/parsing semantic types: {curr_time - start} s')

    print('Writing semantic types...')
    start = time.time()
    cuisty_dir = os.path.join(opts.install_dir, LEVEL_DB)
    dump_cuisty(cuisty, cuisty_dir)
    curr_time = time.time()
    print(f'Writing semantic types: {curr_time - start} s')

    if VERBOSE > 1:
        if nrows is not None and nrows <= 10:
            print(conso)
            print(cuisty)
        print(f'Num unique Terms: {len(conso)}')
        print(f'Num values in Term-CUI/Lat/IsPref dictionary: {sum(len(v) for v in conso.values())}')
        print(f'Size of Term-CUI/Lat/IsPref dictionary: {sys.getsizeof(conso)}')
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')


def parse_args():
    parser = argparse.ArgumentParser(
        prog=__file__,
        description='QuickerUMLS Installation Tool',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        '-l', '--lowercase', action='store_true',
        help='Consider only lowercase version of tokens'
    )

    parser.add_argument(
        '-n', '--normalize-unicode', action='store_true',
        help='Normalize unicode strings to their closest ASCII representation'
    )

    parser.add_argument(
        '-e', '--language', default={'ENG'}, action='append',
        choices={k for k, v in LANGUAGES.items() if v is not None},
        help='Extract concepts of the specified language'
    )

    parser.add_argument(
        '-u', '--umls-dir', required=True,
        help='Directory of UMLS RRF files'
    )

    parser.add_argument(
        '-i', '--install-dir', required=True,
        help='Directory for installing QuickerUMLS files'
    )

    args = parser.parse_args()

    if not os.path.exists(args.install_dir):
        print(f'Creating install directory: {args.install_dir}')
        os.makedirs(args.install_dir)
    elif len(os.listdir(args.install_dir)) > 0:
        err = f"Install directory '{args.install_dir}' is not empty."
        raise FileExistsError(err)
        # NOTE: Only remove files that are required for installation.
        # shutil.rmtree(args.install_dir)
        # os.mkdir(args.install_dir)

    if args.normalize_unicode:
        flag_fp = os.path.join(args.install_dir, 'normalize-unicode.flag')
        open(flag_fp, 'w').close()

    if args.lowercase:
        flag_fp = os.path.join(args.install_dir, 'lowercase.flag')
        open(flag_fp, 'w').close()

    flag_fp = os.path.join(args.install_dir, 'language.flag')
    with open(flag_fp, 'w') as f:
        f.write(os.linesep.join(args.language))

    return args


if __name__ == '__main__':
    t1 = time.time()
    args = parse_args()
    driver(args)
    t2 = time.time()
    print(f'Total runtime: {t2 - t1} s')
