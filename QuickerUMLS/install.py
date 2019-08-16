import os
import sys
import time
import pandas
import shutil
import argparse
import collections
from unidecode import unidecode
from QuickerUMLS.toolbox import CuiSemTypesDB, SimstringDBWriter
from QuickerUMLS.umls_constants import (
    HEADERS_MRCONSO,
    HEADERS_MRSTY,
    LANGUAGES,
    ACCEPTED_SEMTYPES,
)
from QuickerUMLS.helpers import iter_data, data_to_dict


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
    return True if ispref == 'Y' else False
########################################################################


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
