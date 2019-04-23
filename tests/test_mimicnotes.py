import os
import time
from QuickerUMLS import QuickUMLS

num_notes = 100
mimic_notes = f'data/mimic_{num_notes}.txt'
matcher = QuickUMLS('umls-2018-AA')


t0 = time.time()
with open(mimic_notes, 'r') as fd:
    text = fd.read().replace(os.linesep, '')
    # matches = matcher.match(text)
    matches = matcher.match_mp(text)
elapsed = time.time() - t0
print('Time: ', elapsed)
print('Number of notes: ', num_notes)
print('Total words: ', len(text.split()))
print('Number of matches: ', len(matches))
print('Avg. matches per note: ', len(matches) / num_notes)
print('Throughput: ', num_notes / elapsed, ' note/s')
print('Throughput: ', len(matches) / elapsed, ' match/s')
