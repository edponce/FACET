import os
import sys
import time
import pandas
import shutil
import argparse
import collections
from toolbox import CuiSemTypesDB, SimstringDBWriter
from constants import (HEADERS_MRCONSO,
                       HEADERS_MRSTY,
                       LANGUAGES,
                       ACCEPTED_SEMTYPES)


try:
    from unidecode import unidecode
    UNIDECODE_AVAIL = True
except ImportError:
    UNIDECODE_AVAIL = False

# 0 = No status info
# 1 = Processing rate
# 2 = Data structures info
PROFILE = 1


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
              filters=None,
              headers=None,
              valids=None,
              invalids=None,
              multi_converters=None,
              unique_keys=False,
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
        filters (Iterable[str|int]): Columns to load from data, which
            makes them accessible for row filtering, but are not included
            as dictionary values.
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

        multi_converters (Dict[Any:Iterable[Callable]]): Mapping between
            headers and sequences of converter functions to be applied after
            row filtering (and after regular/post-converter functions).

        unique_keys (bool): Control if keys can be repeated or not.

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
    if filters is None:
        filters = ()

    # Get keys/values/filters sizes into variables to prevent calling len()
    # for every iteration during processing.
    # NOTE: Performance-wise, not sure if this is worth it, but makes
    # code cleaner.
    num_keys = len(keys)
    num_values = len(values)
    num_filters = len(filters)
    values_stop = num_keys + num_values

    # Combine columns to use into a single data structure.
    usecols = (*keys, *values, *filters)

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

    # Extend the column headers with a dummy header (hopefully unique).
    # NOTE: UMLS files end with a bar at each line and Pandas assumes
    # there is an extra column afterwards (only required if using the
    # 'python' engine).
    if headers is not None and kwargs.get('engine') == 'python':
        headers = list(headers) + [' ']

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
        reader = [reader]

    # Internal data structure for tracking unique keys.
    keys_processed = set()

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

            # Organize keys
            ks = (kv[0] if num_keys == 1
                  else tuple(kv[:num_keys]))

            # Filter unique keys
            if unique_keys:
                if ks not in keys_processed:
                    keys_processed.add(ks)
                else:
                    continue

            # Organize values
            vs = (kv[num_keys] if num_values == 1
                  else tuple(kv[num_keys:values_stop]))

            yield ks, vs


def umls_to_dict(*args, **kwargs):
    """Load UMLS data into a dictionary.

    Args (see 'iter_umls')

    Kwargs (see 'iter_umls')

    Examples:
        >>> umls = umls_to_dict('MRSTY.RRF', 'cui', 'sty',
                                headers=HEADERS_MRSTY)
        >>> umls = umls_to_dict('MRSTY.RRF', 0, 1)
        >>> umls = umls_to_dict('MRSTY.RRF', 'cui', 'sty',
                                headers=HEADERS_MRSTY,
                                valid_values=ACCEPTED_SEMTYPES)
        >>> umls = umls_to_dict('MRSTY.RRF', 'cui', ['sty', 'hier'],
                                headers=HEADERS_MRSTY)
        >>> umls = umls_to_dict('MRSTY.RRF', 0, [1, 2],
                                nrows=10, valid_values=[None, None])
    """
    unique_keys = kwargs.get('unique_keys', False)
    if unique_keys:
        # Disable 'unique_keys' option for 'iter_umls' so that it does
        # use internal data structure because the dictionary already
        # accounts for that. Values of unique keys are not placed in a set.
        # NOTE: Set value the first time only so that it has same behavior
        # as 'iter_umls'.
        kwargs['unique_keys'] = False
        umls = collections.defaultdict()
        for k, v in iter_umls(*args, **kwargs):
            if k not in umls:
                umls[k] = v
    else:
        umls = collections.defaultdict(set)
        for k, v in iter_umls(*args, **kwargs):
            umls[k].add(v)
    return umls


def dump_cuisty(cuisty,
                cuisty_db,
                bulk_size=1000,
                status_step=100000):
    # Profile
    prev_time = time.time()
    num_terms = 0

    if isinstance(cuisty, dict):
        cuisty = cuisty.items()

    sty_bulk = []
    for i, cui_sty in enumerate(cuisty, start=1):
        if len(sty_bulk) == bulk_size:
            cuisty_db.bulk_insert_sty(sty_bulk)
            sty_bulk = []
        else:
            sty_bulk.append(cui_sty)

        # Profile
        if PROFILE > 0 and i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / (bulk_size * (status_step / bulk_size))} s/batch')
            prev_time = curr_time

    # Flush remaining ones
    if len(sty_bulk) > 0:
        cuisty_db.bulk_insert_sty(sty_bulk)

        # Profile
        if PROFILE > 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / len(sty_bulk)} s/batch')


def dump_conso(conso,
               cuisty_db,
               bulk_size=1000,
               status_step=100000):
    # Profile
    prev_time = time.time()
    num_terms = 0

    if isinstance(conso, dict):
        conso = conso.items()

    terms = set()
    cui_bulk = []
    for i, ((term, cui), preferred) in enumerate(conso, start=1):
        # Profile
        num_terms += 1

        terms.add(term)

        if len(cui_bulk) == bulk_size:
            cuisty_db.safe_bulk_insert_cui(cui_bulk)
            cui_bulk = []
        else:
            cui_bulk.append((term, cui, preferred))

        # Profile
        if PROFILE > 0 and i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / (bulk_size * (status_step / bulk_size))} s/batch')
            prev_time = curr_time

    # Flush remaining ones
    if len(cui_bulk) > 0:
        cuisty_db.safe_bulk_insert_cui(cui_bulk)

        # Profile
        if PROFILE > 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / len(cui_bulk)} s/batch')

    # Profile
    if PROFILE > 1:
        print(f'Num terms: {num_terms}')
        print(f'Num unique terms: {len(terms)}')
        print(f'Size of Simstring terms: {sys.getsizeof(terms)}')

    return terms


def dump_terms(simstring_terms,
               ss_db,
               bulk_size=1,
               status_step=100000):
    # Profile
    prev_time = time.time()

    for i, term in enumerate(simstring_terms, start=1):
        ss_db.insert(term)

        # Profile
        if PROFILE > 0 and i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / (bulk_size * (status_step / bulk_size))} s/batch')
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

    # Database connections
    cuisty_db = CuiSemTypesDB(cuisty_dir)
    ss_db = SimstringDBWriter(simstring_dir)

    # Set converter functions
    converters = collections.defaultdict(list)
    if opts.lowercase:
        converters['str'].append(lowercase_str)
    if opts.normalize_unicode:
        converters['str'].append(normalize_unicode_str)
    converters['ispref'].append(is_preferred)

    print('Loading/parsing concepts...')
    start = time.time()
    conso = iter_umls(mrconso_file, ['str', 'cui'], ['ispref'], filters=['lat'], headers=HEADERS_MRCONSO, valids={'lat': opts.language}, multi_converters=converters, unique_keys=True, nrows=nrows)
    # conso = umls_to_dict(mrconso_file, ['str', 'cui'], ['ispref'], filters=['lat'], headers=HEADERS_MRCONSO, valids={'lat': opts.language}, multi_converters=converters, nrows=nrows)
    curr_time = time.time()
    print(f'Loading/parsing concepts: {curr_time - start} s')

    # Profile
    if PROFILE > 1:
        if nrows is not None and nrows < 20:
            print(conso)
        print(f'Num unique Terms: {len(conso)}')
        print(f'Num values in Term-CUI/Lat/IsPref dictionary: {sum(len(v) for v in conso.values())}')
        print(f'Size of Term-CUI/Lat/IsPref dictionary: {sys.getsizeof(conso)}')

    print('Writing concepts...')
    start = time.time()
    terms = dump_conso(conso, cuisty_db)
    curr_time = time.time()
    print(f'Writing concepts: {curr_time - start} s')

    print('Writing Simstring database...')
    start = time.time()
    dump_terms(terms, ss_db)
    curr_time = time.time()
    print(f'Writing Simstring database: {curr_time - start} s')

    print('Loading/parsing semantic types...')
    start = time.time()
    cuisty = iter_umls(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY, valids={'sty': ACCEPTED_SEMTYPES}, nrows=nrows)
    # cuisty = iter_umls(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY, valids={'cui': {k[1] for k in conso.keys()}, 'sty': ACCEPTED_SEMTYPES}, nrows=nrows)
    # cuisty = umls_to_dict(mrsty_file, ['cui'], ['sty'], headers=HEADERS_MRSTY, valids={'cui': {k[1] for k in conso.keys()}, 'sty': ACCEPTED_SEMTYPES}, nrows=nrows)
    curr_time = time.time()
    print(f'Loading/parsing semantic types: {curr_time - start} s')

    # Profile
    if PROFILE > 1:
        if nrows is not None and nrows <= 10:
            print(cuisty)
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    print('Writing semantic types...')
    start = time.time()
    dump_cuisty(cuisty, cuisty_db)
    curr_time = time.time()
    print(f'Writing semantic types: {curr_time - start} s')


def parse_args():
    parser = argparse.ArgumentParser(
        prog=__file__,
        description='QuickerUMLS Installation Tool',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        '-u', '--umls_dir', required=True,
        help='Directory of UMLS RRF files'
    )

    parser.add_argument(
        '-i', '--install_dir', required=True,
        help='Directory for installing QuickerUMLS files'
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

    args = parser.parse_args()

    if not os.path.exists(args.install_dir):
        print(f'Creating install directory: {args.install_dir}')
        os.makedirs(args.install_dir)
    elif len(os.listdir(args.install_dir)) > 0:
        print(f'Install directory ({args.install_dir}) is not empty,'
              'removing files...')
        shutil.rmtree(args.install_dir)
        os.mkdir(args.install_dir)
        # exit(1)

    if args.normalize_unicode:
        if not UNIDECODE_AVAIL:
            err = ("""'unidecode' is needed for unicode normalization
                   please install it via the 'pip install unidecode'
                   command.""")
            print(err, file=sys.stderr)
            exit(1)
        flag_fp = os.path.join(args.install_dir, 'normalize-unicode.flag')
        open(flag_fp, 'w').close()

    if args.lowercase:
        flag_fp = os.path.join(args.install_dir, 'lowercase.flag')
        open(flag_fp, 'w').close()

    flag_fp = os.path.join(args.install_dir, 'language.flag')
    with open(flag_fp, 'w') as f:
        f.write(os.linesep.join(args.language))

    return args


def write_terms(terms,
                file,
                status_step=100000):
    # Profile
    prev_time = time.time()

    with open(file, 'w') as fd:
        for i, term in enumerate(terms, start=1):
            fd.write(term + '\n')

            # Profile
            if PROFILE > 0 and i % status_step == 0:
                curr_time = time.time()
                print(f'{i}: {curr_time - prev_time} s')
                prev_time = curr_time


def driver_umlsonly(opts):
    # UMLS files
    mrconso_file = os.path.join(opts.umls_dir, 'MRCONSO.RRF')

    # Set converter functions
    converters = collections.defaultdict(list)
    if opts.lowercase:
        converters['str'].append(lowercase_str)
    if opts.normalize_unicode:
        converters['str'].append(normalize_unicode_str)

    print('Loading/parsing concepts...')
    start = time.time()
    conso = umls_to_dict(mrconso_file, ['str'], None, filters=['lat'], headers=HEADERS_MRCONSO, valids={'lat': opts.language}, multi_converters=converters, nrows=opts.rows)
    curr_time = time.time()
    print(f'Loading/parsing concepts: {curr_time - start} s')

    # Profile
    if PROFILE > 1:
        if opts.nrows is not None and opts.rows < 20:
            print(conso)
        print(f'Num unique Terms: {len(conso)}')
        print(f'Size of Term dictionary: {sys.getsizeof(conso)}')

    print('Writing Simstring database...')
    start = time.time()
    write_terms(conso.keys(), opts.file)
    curr_time = time.time()
    print(f'Writing Simstring database: {curr_time - start} s')


def parse_args_umlsonly():
    parser = argparse.ArgumentParser(
        prog=__file__,
        description='QuickerUMLS Installation Tool',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        '-f', '--file', type=str, required=True,
        help='Term file to write UMLS concepts'
    )

    parser.add_argument(
        '-u', '--umls_dir', required=True,
        help='Directory of UMLS RRF files'
    )

    parser.add_argument(
        '-r', '--rows', type=int,
        help='Maximum number of rows to process'
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

    args = parser.parse_args()

    if args.rows is not None and args.rows <= 0:
        print('Error: invalid number of rows to process',
              file=sys.stderr)
        exit(1)

    if args.normalize_unicode:
        if not UNIDECODE_AVAIL:
            err = ("""'unidecode' is needed for unicode normalization
                   please install it via the 'pip install unidecode'
                   command.""")
            print(err, file=sys.stderr)
            exit(1)

    return args

if __name__ == '__main__':
    t1 = time.time()
    args = parse_args_umlsonly()
    driver_umlsonly(args)
    t2 = time.time()
    print(f'Total runtime: {t2 - t1} s')
