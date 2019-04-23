import os
import sys
import time
import shutil
import argparse
from six.moves import input
from toolbox import CuiSemTypesDB, SimstringDBWriter, mkdir
from constants import HEADERS_MRCONSO, HEADERS_MRSTY, LANGUAGES

try:
    from unidecode import unidecode
    UNIDECODE_AVAIL = True
except ImportError:
    UNIDECODE_AVAIL = False


def get_semantic_types(mrsty_path, mrsty_headers):
    sem_types = {}
    with open(mrsty_path, 'r') as f:
        # Parse lines only until the fields we need, ignore remaining
        cui_idx = mrsty_headers.index('cui')
        sty_idx = mrsty_headers.index('sty')
        max_split = max(cui_idx, sty_idx) + 1

        for ln in f:
            content = ln.strip().split('|', max_split)
            sem_types.setdefault(content[cui_idx], set()).add(content[sty_idx].encode('utf-8'))
    return sem_types


def extract_from_mrconso(mrconso_path, sem_types, opts, mrconso_header):
    with open(mrconso_path, 'r') as f:
        # Parse lines only until the fields we need, ignore remaining
        str_idx = mrconso_header.index('str')
        cui_idx = mrconso_header.index('cui')
        pref_idx = mrconso_header.index('ispref')
        lat_idx = mrconso_header.index('lat')
        max_split = max(str_idx, cui_idx, pref_idx, lat_idx) + 1

        processed = set()
        for ln in f:
            content = ln.strip().split('|', max_split)

            if content[lat_idx] != opts.language:
                continue

            cui = content[cui_idx]
            text = content[str_idx].strip()

            if (cui, text) in processed:
                continue

            processed.add((cui, text))

            if opts.lowercase:
                text = text.lower()

            if opts.normalize_unicode:
                text = unidecode(text)

            preferred = 1 if content[pref_idx] else 0

            yield (text, cui, sem_types[cui], preferred)


def parse_and_encode_ngrams(extracted_it, cuisty_dir, bulk_size=1000, status_step=100000):
    # Profile
    prev_time = time.time()

    cuisty_db = CuiSemTypesDB(cuisty_dir)
    simstring_terms = set()
    cui_bulk = []
    sty_bulk = []
    for i, (term, cui, stys, preferred) in enumerate(extracted_it, start=1):
        simstring_terms.add(term)

        if len(cui_bulk) == bulk_size:
            cuisty_db.bulk_insert_cui(cui_bulk)
            cuisty_db.bulk_insert_sty(sty_bulk)
            cui_bulk = []
            sty_bulk = []
        else:
            cui_bulk.append((term, cui, preferred))
            sty_bulk.append((cui, stys))

        # Profile
        if i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')
            sys.stdout.flush()
            prev_time = curr_time

    # Flush remaining ones
    if len(cui_bulk) > 0:
        cuisty_db.bulk_insert_cui(cui_bulk)
        cuisty_db.bulk_insert_sty(sty_bulk)
        cui_bulk = []
        sty_bulk = []

        # Profile
        curr_time = time.time()
        print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')
        sys.stdout.flush()

    return simstring_terms


def dump_simstrings(simstring_dir, simstring_terms, status_step=10000):
    # Profile
    prev_time = time.time()

    ss_db = SimstringDBWriter(simstring_dir)
    for i, term in enumerate(simstring_terms, start=1):
        ss_db.insert(term)

        # Profile
        if i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')
            sys.stdout.flush()
            prev_time = curr_time


def driver(opts):
    # UMLS files
    mrconso_path = os.path.join(opts.umls_installation_path, 'MRCONSO.RRF')
    mrsty_path = os.path.join(opts.umls_installation_path, 'MRSTY.RRF')

    # Create destination directories for the two databases
    simstring_dir = os.path.join(opts.destination_path, 'umls-simstring.db')
    cuisty_dir = os.path.join(opts.destination_path, 'cui-semtypes.db')
    mkdir(simstring_dir)
    mkdir(cuisty_dir)

    print('Loading semantic types...')
    sys.stdout.flush()
    start = time.time()
    sem_types = get_semantic_types(mrsty_path, HEADERS_MRSTY)
    curr_time = time.time()
    print(f'Loading semantic types: {curr_time - start} s')
    print(f'Number of unique CUIs: {len(sem_types)}')

    print('Loading and parsing concepts...')
    sys.stdout.flush()
    start = time.time()
    mrconso_iterator = extract_from_mrconso(mrconso_path, sem_types, opts, HEADERS_MRCONSO)
    simstring_terms = parse_and_encode_ngrams(mrconso_iterator, cuisty_dir)
    curr_time = time.time()
    print(f'Loading and parsing concepts: {curr_time - start} s')
    print(f'Number of unique (CUI, term): {len(simstring_terms)}')

    print('Writing Simstring database...')
    sys.stdout.flush()
    start = time.time()
    dump_simstrings(simstring_dir, simstring_terms)
    curr_time = time.time()
    print(f'Writing Simstring database: {curr_time - start} s')



if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument(
        'umls_installation_path',
        help=('Location of UMLS installation files (`MRCONSO.RRF` and '
              '`MRSTY.RRF` files)')
    )
    ap.add_argument(
        'destination_path',
        help='Location where the necessary QuickUMLS files are installed'
    )
    ap.add_argument(
        '-L', '--lowercase', action='store_true',
        help='Consider only lowercase version of tokens'
    )
    ap.add_argument(
        '-U', '--normalize-unicode', action='store_true',
        help='Normalize unicode strings to their closest ASCII representation'
    )
    ap.add_argument(
        '-E', '--language', default='ENG', choices=LANGUAGES,
        help='Extract concepts of the specified language'
    )
    opts = ap.parse_args()

    if not os.path.exists(opts.destination_path):
        msg = ('directory "{}" does not exists; should i create it? [y/n] '
               ''.format(opts.destination_path))
        create = input(msg).lower().strip() == 'y'
        if not create:
            print('aborting.')
            exit(1)
        os.makedirs(opts.destination_path)

    if len(os.listdir(opts.destination_path)) > 0:
        msg = ('directory "{}" is not empty; should i empty it? [y/n] '
               ''.format(opts.destination_path))
        empty = input(msg).lower().strip() == 'y'
        if not empty:
            print('aborting.')
            exit(1)
        shutil.rmtree(opts.destination_path)
        os.mkdir(opts.destination_path)

    if opts.normalize_unicode:
        if not UNIDECODE_AVAIL:
            err = ("""'unidecode' is needed for unicode normalization
                   please install it via the 'pip install unidecode'
                   command.""")
            print(err, file=sys.stderr)
            exit(1)
        flag_fp = os.path.join(opts.destination_path, 'normalize-unicode.flag')
        open(flag_fp, 'w').close()

    if opts.lowercase:
        flag_fp = os.path.join(opts.destination_path, 'lowercase.flag')
        open(flag_fp, 'w').close()

    flag_fp = os.path.join(opts.destination_path, 'language.flag')
    with open(flag_fp, 'w') as f:
        f.write(opts.language)

    driver(opts)
