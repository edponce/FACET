import os
import sys
import time
import shutil
import argparse
from toolbox import CuiSemTypesDB, SimstringDBWriter
from constants import HEADERS_MRCONSO, HEADERS_MRSTY, LANGUAGES

try:
    from unidecode import unidecode
    UNIDECODE_AVAIL = True
except ImportError:
    UNIDECODE_AVAIL = False


def extract_mrsty(mrsty_path, mrsty_headers=HEADERS_MRSTY):
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


def extract_mrconso(mrconso_path, sem_types, mrconso_header=HEADERS_MRCONSO,
                    lowercase=False,
                    normalize_unicode=False,
                    language=['ENG']):
    with open(mrconso_path, 'r') as f:
        # Parse lines only until the fields we need, ignore remaining
        str_idx = mrconso_header.index('str')
        cui_idx = mrconso_header.index('cui')
        pref_idx = mrconso_header.index('ispref')
        lat_idx = mrconso_header.index('lat')
        max_split = max(str_idx, cui_idx, pref_idx, lat_idx) + 1

        processed = set()

        # Profile
        num_valid_lang = 0
        num_repeated_cuitext = 0
        num_lines = 0

        for ln in f:
            # Profile
            num_lines += 1

            content = ln.strip().split('|', max_split)

            if content[lat_idx] in language:
                continue

            # Profile
            num_valid_lang += 1

            cui = content[cui_idx]
            text = content[str_idx].strip()

            if (cui, text) in processed:
                # Profile
                num_repeated_cuitext += 1
                continue

            processed.add((cui, text))

            if lowercase:
                text = text.lower()

            if normalize_unicode:
                text = unidecode(text)

            preferred = 1 if content[pref_idx] else 0

            yield (text, cui, sem_types[cui], preferred)

        # Profile
        print(f'Num lines to process: {num_lines}')
        print(f'Num valid language: {num_valid_lang}')
        print(f'Num repeated CUI-term: {num_repeated_cuitext}')
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
        if i % status_step == 0:
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
        curr_time = time.time()
        print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')

    # Profile
    print(f'Num terms: {num_terms}')
    print(f'Num unique terms: {len(terms)}')

    return terms


def dump_terms(simstring_dir, simstring_terms,
               status_step=100000):
    # Profile
    prev_time = time.time()

    ss_db = SimstringDBWriter(simstring_dir)
    for i, term in enumerate(simstring_terms, start=1):
        ss_db.insert(term)

        # Profile
        if i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')
            prev_time = curr_time


def driver(opts):
    # UMLS files
    mrconso_path = os.path.join(opts.umls_installation_path, 'MRCONSO.RRF')
    mrsty_path = os.path.join(opts.umls_installation_path, 'MRSTY.RRF')

    # Create destination directories for the two databases
    simstring_dir = os.path.join(opts.destination_path, 'umls-simstring.db')
    cuisty_dir = os.path.join(opts.destination_path, 'cui-semtypes.db')
    os.makedirs(simstring_dir)
    os.makedirs(cuisty_dir)

    print('Loading semantic types...')
    start = time.time()
    cuisty_dict = extract_mrsty(mrsty_path)
    curr_time = time.time()
    print(f'Loading semantic types: {curr_time - start} s')
    print(f'Num unique CUIs: {len(cuisty_dict)}')
    print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty_dict.values())}')
    print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty_dict)}')

    print('Loading and parsing concepts...')
    start = time.time()
    conso_cuisty_iter = extract_mrconso(
                            mrconso_path, cuisty_dict,
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
        '-N', '--normalize-unicode', action='store_true',
        help='Normalize unicode strings to their closest ASCII representation'
    )
    ap.add_argument(
        '-E', '--language', default=['ENG'], action='append', choices=LANGUAGES,
        help='Extract concepts of the specified language'
    )
    clargs = ap.parse_args()

    if not os.path.exists(clargs.destination_path):
        msg = ('directory "{}" does not exists; should i create it? [y/n] '
               ''.format(clargs.destination_path))
        create = input(msg).lower().strip() == 'y'
        if not create:
            print('aborting.')
            exit(1)
        os.makedirs(clargs.destination_path)

    if len(os.listdir(clargs.destination_path)) > 0:
        msg = ('directory "{}" is not empty; should i empty it? [y/n] '
               ''.format(clargs.destination_path))
        empty = input(msg).lower().strip() == 'y'
        if not empty:
            print('aborting.')
            exit(1)
        shutil.rmtree(clargs.destination_path)
        os.makedirs(clargs.destination_path)

    if clargs.normalize_unicode:
        if not UNIDECODE_AVAIL:
            err = ("""'unidecode' is needed for unicode normalization
                   please install it via the 'pip install unidecode'
                   command.""")
            print(err, file=sys.stderr)
            exit(1)
        flag_fp = os.path.join(clargs.destination_path, 'normalize-unicode.flag')
        open(flag_fp, 'w').close()

    if clargs.lowercase:
        flag_fp = os.path.join(clargs.destination_path, 'lowercase.flag')
        open(flag_fp, 'w').close()

    flag_fp = os.path.join(clargs.destination_path, 'language.flag')
    with open(flag_fp, 'w') as f:
        f.write(os.linesep.join(clargs.language))

    start = time.time()
    driver(clargs)
    curr_time = time.time()
    print(f'Total runtime: {curr_time - start} s')
