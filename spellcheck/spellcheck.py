#!/usr/bin/env python3


import os
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
    ss_db = toolbox.SimstringDBReader(args.database, 'jaccard', 0.7)
    t2 = time.time()
    print(f"Elapsed time: {t2 - t1} s")

    t1 = time.time()
    with open(args.input) as ifd, open(args.output, 'w') as ofd:
        for line in ifd:
            for word in line.split():
                suggestions = ss_db.get(word)

                if len(suggestions) == 0:
                    ofd.write(word)
                else:
                    ofd.write(suggestions[0])
                ofd.write(' ')

    t2 = time.time()
    print(f"Elapsed time: {t2 - t1} s")


if __name__ == "__main__":
    args = parse_args()
    main(args)
