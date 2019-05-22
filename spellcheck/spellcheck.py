#!/usr/bin/env python3


import os
import re
import sys
import time
import argparse
from QuickerUMLS import toolbox


def parse_args():
    parser = argparse.ArgumentParser(
        prog=__file__,
        description='Simstring Spell-checker Driver',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        '-d', '--database', type=str, required=True,
        help='Simstring database'
    )

    parser.add_argument(
        '-i', '--input', type=str, required=True,
        help='Input corpus file'
    )

    parser.add_argument(
        '-o', '--output', type=str, required=True,
        help='Output file of spell-checked corpus'
    )

    args = parser.parse_args()

    if not os.path.exists(args.input) or not os.path.isfile(args.input):
        print('Error: input file does not exists '
              f'or is not a valid file: {args.input}',
              file=sys.stderr)
        exit(1)

    if not os.path.exists(args.database) or not os.path.isdir(args.database):
        print('Error: simstring databse does not exists '
              f'or is not a valid directory: {args.database}',
              file=sys.stderr)
        exit(1)

    return args


def main(args):
    t1 = time.time()
    ss_db = toolbox.SimstringDBReader(args.database, 'jaccard', 0.8)
    t2 = time.time()
    print(f"Elapsed time: {t2 - t1} s")

    t1 = time.time()
    with open(args.input) as ifd, open(args.output, 'w') as ofd:
        for line in ifd:
            # Split on single whitespace
            # Leave remaining syntax as is
            word_parts = line.split(' ')
            for i, words in enumerate(word_parts):
                # Consecutive empty spaces
                if not words:
                    ofd.write(' ')
                    continue
                # Tab/EOL surrounded by empty spaces
                elif words in ('\t', '\n'):
                    ofd.write(words)
                    continue

                # Tab/EOL embedded with word
                idxs = []
                idxs.extend([m.start() for m in re.finditer('\n', words)])
                idxs.extend([m.start() for m in re.finditer('\t', words)])

                if len(idxs) > 0:
                    idxs.sort()
                else:
                    # Length of word because the following stage uses
                    # slice syntax which is non-inclusive end.
                    idxs = [len(words)]

                # Split word based on tab/EOL
                e = -1
                for idx in idxs:
                    s = e + 1
                    e = idx

                    # Single tab/EOL
                    if s == e:
                        ofd.write(words[s])
                        continue

                    word = words[s:e]
                    suggestions = ss_db.get(word)
                    if len(suggestions) == 0:
                        ofd.write(word)
                    else:
                        ofd.write(suggestions[0])

                    # If not last word part, write tab/EOL
                    if e < len(words):
                        ofd.write(words[e])

                # Write space removed by split()
                if i < len(word_parts) - 1:
                    ofd.write(' ')
    t2 = time.time()
    print(f"Elapsed time: {t2 - t1} s")


if __name__ == "__main__":
    args = parse_args()
    main(args)
