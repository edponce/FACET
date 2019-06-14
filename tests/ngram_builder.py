#! /usr/bin/env python3


import re
import os
import sys
import time
import argparse

try:
    from unidecode import unidecode
    UNIDECODE_AVAIL = True
except ImportError:
    UNIDECODE_AVAIL = False


STOP_WORDS = [
    'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves',
    'you', 'you"re', 'you"ve', 'you"ll', 'you"d', 'your', 'yours', 'yourself',
    'yourselves', 'he', 'him', 'his', 'himself', 'she', 'she"s', 'her', 'hers',
    'herself', 'it', 'it"s', 'its', 'itself', 'they', 'them', 'their',
    'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that',
    'that"ll', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be',
    'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did',
    'doing', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as',
    'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against',
    'between', 'into', 'through', 'during', 'before', 'after', 'above',
    'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over',
    'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when',
    'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most',
    'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so',
    'than', 'too', 'very', 'can', 'will', 'just', 'don', 'don"t', 'should',
    'should"ve', 'now', 'll', 're', 've', 'ain', 'aren', 'aren"t', 'couldn',
    'couldn"t', 'didn', 'didn"t', 'doesn', 'doesn"t', 'hadn', 'hadn"t',
    'hasn', 'hasn"t', 'haven', 'haven"t', 'isn', 'isn"t', 'ma', 'mightn',
    'mightn"t', 'mustn', 'mustn"t', 'needn', 'needn"t', 'shan', 'shan"t',
    'shouldn', 'shouldn"t', 'wasn', 'wasn"t', 'weren', 'weren"t', 'won',
    'won"t', 'wouldn', 'wouldn"t'
]

STOP_SYMBOLS = ' \t!"#$%&\'*+,-./:;<=>?@\\^`|~()[]{}'


def isnumber(x):
    try:
        float(x)
        return True
    except ValueError:
        return False


def driver(opts):
    # Select files to process from directory
    input_files = opts.input
    input_fullfiles = []
    if input_files is None:
        input_files = os.listdir(opts.dir)
    for file in input_files:
        if opts.dir is None:
            input_fullfiles.append(file)
        else:
            input_fullfiles.append(os.path.join(opts.dir, file))

    # Tokenize and build n-grams
    ngrams = []
    pattern = re.compile(',*\s+')
    for file in input_fullfiles:
        with open(file) as fd:
            for line in fd:
                word_list = re.split(pattern, line.rstrip())
                for word in word_list:
                    if UNIDECODE_AVAIL and opts.normalize_unicode:
                        word = unidecode(word).strip(STOP_SYMBOLS)
                    else:
                        word = word.strip(STOP_SYMBOLS)

                    if opts.lowercase:
                        word = word.lower()

                    if len(word) > 1 \
                       and word not in STOP_WORDS \
                       and not isnumber(word):
                        ngrams.append(word)

    # Write n-grams
    with open(opts.output, 'w') as fd:
        for word in ngrams:
            fd.write('{}\n'.format(word))


def parse_opts():
    parser = argparse.ArgumentParser(
        prog=__file__,
        description='N-gram Builder Tool',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        '-d', '--dir', required=False,
        help='Directory of input files'
    )

    parser.add_argument(
        '-i', '--input', required=False, nargs='+',
        help='List of input files'
    )

    parser.add_argument(
        '-o', '--output', required=True,
        help='Output file'
    )

    parser.add_argument(
        '-l', '--lowercase', action='store_true',
        help='Consider only lowercase version of tokens'
    )

    parser.add_argument(
        '-n', '--normalize-unicode', action='store_true',
        help='Normalize unicode strings to their closest ASCII representation'
    )

    opts = parser.parse_args()

    if opts.dir is None and opts.input is None:
        err = ("""Invalid configuration, need to provide at least either
               input directory/file.""")
        print(err, file=sys.stderr)
        sys.exit(1)

    if opts.normalize_unicode:
        if not UNIDECODE_AVAIL:
            err = ("""'unidecode' is needed for unicode normalization
                   please install it via the 'pip install unidecode'
                   command.""")
            print(err, file=sys.stderr)
            sys.exit(1)

    return opts

if __name__ == '__main__':
    t1 = time.time()
    opts = parse_opts()
    driver(opts)
    t2 = time.time()
    print(f'Total runtime: {t2 - t1} s')
