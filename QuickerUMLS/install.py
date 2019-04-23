from __future__ import unicode_literals, division, print_function

# built in modules
import os
import sys
import time
import codecs
import shutil
import argparse
from six.moves import input

# project modules
from toolbox import CuiSemTypesDB, SimstringDBWriter, mkdir
from constants import HEADERS_MRCONSO, HEADERS_MRSTY, LANGUAGES

try:
    from unidecode import unidecode
except ImportError:
    pass


def get_semantic_types(path, headers):
    cui_idx = headers.index('cui')
    sty_idx = headers.index('sty')

    # Parse lines only until the fields we need, ignore remaining
    max_split = max(cui_idx, sty_idx) + 1
    sem_types = {}
    with open(path, 'r') as f:
        for ln in f:
            content = ln.strip().split('|', max_split)
            sem_types.setdefault(content[cui_idx], set()).add(content[sty_idx].encode('utf-8'))
    return sem_types


def get_mrconso_iterator(path, headers, lang='ENG'):
    with codecs.open(path, encoding='utf-8') as f:
        for ln in f:
            content = dict(zip(headers, ln.strip().split('|')))

            if content['lat'] != lang:
                continue

            yield content


def extract_from_mrconso(
        mrconso_path, mrsty_path, opts,
        mrconso_header=HEADERS_MRCONSO, mrsty_header=HEADERS_MRSTY):

    print('Loading semantic types...', end=' ')
    sys.stdout.flush()
    start = time.time()
    sem_types = get_semantic_types(mrsty_path, mrsty_header)
    curr_time = time.time()
    print(f'done in {curr_time - start} s')

    print('Loading concepts...')
    start = time.time()
    prev_time = start

    with open(mrconso_path, 'r') as f:
        # Parse lines only until the fields we need, ignore remaining
        str_idx = mrconso_header.index('str')
        cui_idx = mrconso_header.index('cui')
        pref_idx = mrconso_header.index('ispref')
        lat_idx = mrconso_header.index('lat')
        max_split = max(str_idx, cui_idx, pref_idx, lat_idx) + 1

        status_step = 100000
        processed = set()
        i = 0
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
            i += 1

            # Print timing after 'continue' statements
            if i % status_step == 0:
                curr_time = time.time()
                print(f'{i}: {curr_time - start} s, {(curr_time - prev_time) / status_step} s/term')
                sys.stdout.flush()
                prev_time = curr_time

    curr_time = time.time()
    print(f'COMPLETED: {curr_time - start} s')
    print('Processed: ', len(processed))


def parse_and_encode_ngrams(extracted_it, simstring_dir, cuisty_dir):
    # Create destination directories for the two databases
    mkdir(simstring_dir)
    mkdir(cuisty_dir)

    ss_db = SimstringDBWriter(simstring_dir)
    cuisty_db = CuiSemTypesDB(cuisty_dir)

    db_bulk = 500
    simstring_terms = set()

    cui_bulk = []
    sty_bulk = []
    for i, (term, cui, stys, preferred) in enumerate(extracted_it, start=1):
        simstring_terms.add(term)

        if i % db_bulk == 0:
            cuisty_db.bulk_insert_cui(cui_bulk)
            cuisty_db.bulk_insert_sty(sty_bulk)
            cui_bulk = []
            sty_bulk = []
        else:
            cui_bulk.append((term, cui, preferred))
            sty_bulk.append((cui, stys))

    # Flush remaining ones
    if len(cui_bulk) > 0:
        cuisty_db.bulk_insert_cui(cui_bulk)
        cuisty_db.bulk_insert_sty(sty_bulk)
        cui_bulk = []
        sty_bulk = []

    for term in simstring_terms:
        ss_db.insert(term)


def driver(opts):
    if not os.path.exists(opts.destination_path):
        msg = ('Directory "{}" does not exists; should I create it? [y/N] '
               ''.format(opts.destination_path))
        create = input(msg).lower().strip() == 'y'

        if create:
            os.makedirs(opts.destination_path)
        else:
            print('Aborting.')
            exit(1)

    if len(os.listdir(opts.destination_path)) > 0:
        msg = ('Directory "{}" is not empty; should I empty it? [y/N] '
               ''.format(opts.destination_path))
        empty = input(msg).lower().strip() == 'y'
        if empty:
            shutil.rmtree(opts.destination_path)
            os.mkdir(opts.destination_path)
        else:
            print('Aborting.')
            exit(1)

    if opts.normalize_unicode:
        try:
            unidecode
        except NameError:
            err = ('`unidecode` is needed for unicode normalization'
                   'please install it via the `[sudo] pip install '
                   'unidecode` command.')
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

    mrconso_path = os.path.join(opts.umls_installation_path, 'MRCONSO.RRF')
    mrsty_path = os.path.join(opts.umls_installation_path, 'MRSTY.RRF')

    mrconso_iterator = extract_from_mrconso(mrconso_path, mrsty_path, opts)

    simstring_dir = os.path.join(opts.destination_path, 'umls-simstring.db')
    cuisty_dir = os.path.join(opts.destination_path, 'cui-semtypes.db')

    parse_and_encode_ngrams(mrconso_iterator, simstring_dir, cuisty_dir)


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

driver(opts)
