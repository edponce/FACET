import sys
import time
import pandas
import collections


HEADERS_MRSTY = [
    'cui', 'sty', 'hier', 'desc', 'sid', 'num'
]


fn = 'data/MRSTY.RRF'

method = 0
chunk_size = 100000

start = time.time()
if method == 0:
    df = pandas.read_csv(fn, delimiter='|', names=HEADERS_MRSTY, usecols=['cui', 'sty'], index_col=False, memory_map=True, engine='c')

    cuisty = collections.defaultdict(set)
    for cui, sty in zip(df.cui, df.sty):
        cuisty[cui].add(sty)
else:
    reader = pandas.read_csv(fn, delimiter='|', names=HEADERS_MRSTY, usecols=['cui', 'sty'], index_col=False, memory_map=True, engine='c', chunksize=chunk_size)

    cuisty = collections.defaultdict(set)
    for df in reader:
        for cui, sty in zip(df.cui, df.sty):
            cuisty[cui].add(sty)
curr_time = time.time()

print(f'Num unique CUIs: {len(cuisty)}')
print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')
print(f'Total runtime: {curr_time - start} s')

