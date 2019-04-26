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


# Converter functions
def lowercase_str(term):
    return term.lower()


def normalize_unicode_str(term):
    return unidecode(term)


def lowercase_and_normalize_unicode_str(term):
    return normalize_unicode_str(lowercase_str(term))


def is_preferred(ispref):
    return 1 if ispref == 'Y' else 0


def umls_to_dataframe(rrf_file,
                      key,
                      values, *,
                      headers=None,
                      valid_keys=None,
                      valid_values=None,
                      **kwargs):
    """Load UMLS data into a dataframe.

    Uses Pandas 'read_csv' to load data into a dataframe.

    Args:
        rrf_file (str): Path of UMLS RRF file

        key (str|int): Column to use as dictionary key.
            If str, then it corresponds to a 'headers' value.
            If int, then it corresponds to a column index.

        values (str|int|Iterable[str|int]): Columns to use as dictionary values.
            Multiple values are stored as key/tuples in same order as given.
            If str, then it corresponds to 'headers' values.
            If int, then it corresponds to column indices.

    Kwargs:
        headers (Iterable[str]): Column names.
            Headers are required when key/values are str.
            Headers do not need to be complete, but do need to be in order
            and contain the key/values str.

        valid_keys (Iterable[Any]): Valid keys to include.
            If None, then all keys are included.

        valid_values (Iterable[Any|Iterable[Any]]): Valid values to include.
            If None, then all values are included.

    Kwargs (See Pandas read_csv()):
        delimiter
        encoding
        converters
        chunksize
        memory_map
        engine
        skiprows
        nrows

    Examples:
        >>> umls = umls_to_dataframe('MRSTY.RRF', 'cui', 'sty', headers=HEADERS_MRSTY)
        >>> umls = umls_to_dataframe('MRSTY.RRF', 0, 1)
        >>> umls = umls_to_dataframe('MRSTY.RRF', 'cui', 'sty', headers=HEADERS_MRSTY, valid_values=ACCEPTED_SEMTYPES)
        >>> umls = umls_to_dataframe('MRSTY.RRF', 'cui', ['sty', 'hier'], headers=HEADERS_MRSTY)
        >>> umls = umls_to_dataframe('MRSTY.RRF', 0, [1, 2], nrows=10, valid_values=[None, None])
    """
    if isinstance(values, (str, int)):
        values = [values]

    # Extend the column headers with an empty header.
    # NOTE: UMLS files end with a bar at each line and Pandas assumes
    # there is an extra column afterwards (only required if using the
    # 'python' engine).
    if headers is not None:
        headers = list(headers) + ['_']

    # No row filtering
    if valid_keys is None and valid_values is None:
        df_iterator = False
    else:
        df_iterator = True

    reader = pandas.read_csv(rrf_file,
                             delimiter=kwargs.get('delimiter', '|'),
                             names=headers,
                             usecols=[key, *values],

                             # Constant settings
                             header=None,
                             index_col=False,
                             na_filter=False,

                             # Extra
                             encoding=kwargs.get('encoding', 'utf-8'),
                             converters=kwargs.get('converters'),

                             # Performance parameters
                             iterator=df_iterator,
                             chunksize=kwargs.get('chunksize'),
                             memory_map=kwargs.get('memory_map', True),
                             engine=kwargs.get('engine', 'c'), # 'c', 'python'

                             # Debug
                             skiprows=kwargs.get('skiprows'),
                             nrows=kwargs.get('nrows'))

    # No row filtering
    if not df_iterator:
        df = reader
        # df = reader.groupby(key).agg(set)
        # df = reader.groupby(key)
    else:
        df = pandas.DataFrame()
    #     if valid_keys is not None:
    #         df = pandas.concat([chunk[chunk[key] in valid_keys] for chunk in reader], ignore_index=True, sort=False)
    #     if valid_values is not None:
    #         df = pandas.concat([chunk[chunk[values] in valid_values] for chunk in reader], ignore_index=True, sort=False)
    #
    return df


def umls_to_dict(rrf_file,
                 key,
                 values, *,
                 headers=None,
                 valid_keys=None,
                 valid_values=None,
                 **kwargs):
    """Load UMLS data into a dictionary.

    Uses Pandas 'read_csv' to load data into a dataframe which is then
    converted to a dictionary.

    Args:
        rrf_file (str): Path of UMLS RRF file

        key (str|int|Iterable[str|int]): Column to use as dictionary key.
            If str, then it corresponds to a 'headers' value.
            If int, then it corresponds to a column index.

        values (str|int|Iterable[str|int]): Columns to use as dictionary values.
            Multiple values are stored as key/tuples in same order as given.
            If str, then it corresponds to 'headers' values.
            If int, then it corresponds to column indices.

    Kwargs:
        headers (Iterable[str]): Column names.
            Headers are required when key/values are str.
            Headers do not need to be complete, but do need to be in order
            and contain the key/values str.

        valid_keys (Iterable[Any]): Valid keys to include.
            If None, then all keys are included.

        valid_values (Iterable[Any|Iterable[Any]]): Valid values to include.
            If None, then all values are included.

    Kwargs (See Pandas read_csv()):
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
    if isinstance(values, (str, int)):
        values = [values]

    # HACK: Extend the column headers with an empty header.
    # NOTE: UMLS files end with a bar at each line and Pandas assumes
    # there is an extra column afterwards (only required if using the
    # 'python' engine).
    if headers is not None:
        headers = list(headers) + ['_']

    # If 'valid_keys/valid_values' are provided, then apply converter
    # functions after row filtering occurs. An iterable of only Nones is
    # considered as no filtering.
    # For large files, this may help improve performance.
    post_convert = False
    if valid_keys is not None:
        if isinstance(valid_keys, (list, set, tuple)) and any(valid_keys):
            post_convert = True
    if valid_values is not None:
        if isinstance(valid_values, (list, set, tuple)) and any(valid_values):
            post_convert = True

    # If necessary, move converters to post loading data
    post_converters = None
    if post_convert:
        post_converters = kwargs.get('converters')
        kwargs['converters'] = None

    reader = pandas.read_csv(rrf_file,
                             delimiter=kwargs.get('delimiter', '|'),
                             names=headers,
                             usecols=[key, *values],

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
                             engine=kwargs.get('engine', 'c'), # 'c', 'python'

                             # Constant settings
                             header=None,
                             index_col=False,
                             na_filter=False
                             )

    # Place the dataframe into an iterable even if not iterating and
    # chunking through the file, so that it uses the same logic
    # as if it was a TextFileReader object.
    if not isinstance(reader, pandas.io.parsers.TextFileReader):
        reader = [reader]

    umls = collections.defaultdict(set)
    if len(values) == 1:
        values = values[0]
        for df in reader:
            for k, v in zip(df[key], df[values]):
                if valid_keys is not None and k not in valid_keys:
                    continue
                if valid_values is not None and v not in valid_values:
                    continue
                if post_converters is not None:
                    if key in post_converters:
                        k = post_converters[key](k)
                    if values in post_converters:
                        v = post_converters[values](v)
                umls[k].add(v)
    else:
        for df in reader:
            value = (df[val] for val in values)
            for k, *vs in zip(df[key], *value):
                if valid_keys is not None and k not in valid_keys:
                    continue
                if valid_values is not None:
                    is_invalid_value = False
                    for v, f in zip(vs, valid_values):
                        if f is not None and v not in f:
                            is_invalid_value = True
                            break
                    if is_invalid_value:
                        continue
                if post_converters is not None:
                    if key in post_converters:
                        k = post_converters[key](k)
                    vs = (post_converters[val](v)
                          if val in post_converters else v
                          for v, val in zip(vs, values))
                umls[k].add(tuple(vs))

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

    print('Loading semantic types...')
    start = time.time()
    # cuisty = umls_to_dict(mrsty_file, 'cui', 'sty', headers=HEADERS_MRSTY)
    cuisty = umls_to_dict(mrsty_file, 'cui', 'sty', headers=HEADERS_MRSTY, valid_values=ACCEPTED_SEMTYPES)
    curr_time = time.time()
    print(f'Loading semantic types: {curr_time - start} s')

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

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
