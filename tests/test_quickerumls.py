import os
import time
from QuickerUMLS import QuickUMLS


matcher = QuickUMLS('umls-2018-AA')
# matcher = QuickUMLS('umls-2018-AA-fast')

use_list = False


if use_list:
    # 4960 lines
    # umls-2018-AA
    # 36.4 seconds (QuickUMLS)
    # 62.5 seconds (QuickerUMLS)
    with open('data/VA_My_HealtheVet_Blue_Button_Sample_Version_12_10.txt', 'r') as fd:
        textlines = fd.readlines()

    t0 = time.time()
    for ln, text in enumerate(textlines):
        matches = matcher.match(text, best_match=True, ignore_syntax=False)
        print(f'Line {ln}: {len(matches)} matches')
    t1 = time.time()
    print('Time: ', t1 - t0)
else:
    # 4960 lines
    # umls-2018-AA
    # 39.1 seconds (QuickUMLS)
    # 15.3 seconds (QuickerUMLS)
    with open('data/VA_My_HealtheVet_Blue_Button_Sample_Version_12_10.txt', 'r') as fd:
        text = fd.read().replace(os.linesep, '')
    t0 = time.time()
    matches = matcher.match(text, best_match=True, ignore_syntax=False)
    t1 = time.time()
    print(matches)
    print('Time: ', t1 - t0)
