import os
import time
from QuickerUMLS import QuickUMLS
import multiprocessing


matcher = QuickUMLS('umls-2018-AA')
matcher2 = QuickUMLS('umls-2018-AA2')

best_match = True
ignore_syntax = False



# with open('data/VA_My_HealtheVet_Blue_Button_Sample_Version_12_10.txt', 'r') as fd:
with open('data/test.txt', 'r') as fd:
    text = fd.read().replace(os.linesep, '')

chunk = len(text) // 2
t0 = time.time()
p = multiprocessing.Process(target=matcher2.match, args=(text[:chunk],))
p.start()
matches = matcher.match(text[chunk:], best_match=best_match, ignore_syntax=ignore_syntax)
# matches = matcher.match2(text, best_match=best_match, ignore_syntax=ignore_syntax)
p.join()
t1 = time.time()
# print(matches)
print('Number of matches: ', len(matches))
print('Time: ', t1 - t0)
