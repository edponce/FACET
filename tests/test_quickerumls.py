import os
import time
from QuickerUMLS import QuickUMLS


matcher = QuickUMLS('UMLS-2018-AA')

use_list = False
best_match = True
ignore_syntax = False


with open('data/test.txt', 'r') as fd:
    if use_list:
        textlines = fd.readlines()
        t0 = time.time()
        for ln, text in enumerate(textlines):
            matches = matcher.match(text, best_match=best_match, ignore_syntax=ignore_syntax)
        t1 = time.time()
    else:
        text = fd.read().replace(os.linesep, '')
        t0 = time.time()
        matches = matcher.match(text, best_match=best_match, ignore_syntax=ignore_syntax)
        # matches = matcher.match_mp(text, best_match=best_match, ignore_syntax=ignore_syntax)
        t1 = time.time()
print('Number of matches: ', len(matches))
print(matches)
print('Time: ', t1 - t0)
