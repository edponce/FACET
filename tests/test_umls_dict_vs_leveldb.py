import os
import sys
import time
import QuickerUMLS
import collections
from unidecode import unidecode


def run():
    ########################################################################
    # User options
    ########################################################################
    lowercase = True
    normalize_unicode = True
    language = 'ENG'
    nrows = None
    umls_dir = '../../umls/2018-AA'
    mrconso_file = os.path.join(umls_dir, 'MRCONSO.RRF')
    mrsty_file = os.path.join(umls_dir, 'MRSTY.RRF')


    ########################################################################
    # Converter functions
    ########################################################################
    def lowercase_str(term):
        return term.lower()


    def normalize_unicode_str(term):
        return unidecode(term)


    def is_preferred(ispref):
        return 1 if ispref == 'Y' else 0


    # Set converter functions
    converters = collections.defaultdict(list)
    if lowercase:
        converters['str'].append(lowercase_str)
    if normalize_unicode:
        converters['str'].append(normalize_unicode_str)
    # converters['ispref'].append(is_preferred)


    ########################################################################
    # Load UMLS
    ########################################################################
    t0 = time.time()
    # conso = QuickerUMLS.install.data_to_dict(mrconso_file, ['str', 'cui'], ['ispref'], filters=['lat'], headers=QuickerUMLS.constants.HEADERS_MRCONSO, valids={'lat': language}, converters=converters, nrows=nrows)
    conso = QuickerUMLS.install.data_to_dict(mrconso_file, ['str'], ['cui'], headers=QuickerUMLS.constants.HEADERS_MRCONSO, valids={'lat': language, 'ispref': 'Y'}, converters=converters, nrows=nrows)  # 320 MB
    t1 = time.time()
    print('Loading UMLS MRCONSO.RRF to dictionary (s): ', t1 - t0)
    print('Size of UMLS MRCONSO dictionary (B): ', sys.getsizeof(conso))

    t0 = time.time()
    # sty = QuickerUMLS.install.data_to_dict(mrsty_file, ['cui'], ['sty'], headers=QuickerUMLS.constants.HEADERS_MRSTY, valids={'sty': QuickerUMLS.constants.ACCEPTED_SEMTYPES}, nrows=nrows)
    sty = QuickerUMLS.install.data_to_dict(mrsty_file, ['cui'], ['sty'], headers=QuickerUMLS.constants.HEADERS_MRSTY, valids={'sty': QuickerUMLS.constants.ACCEPTED_SEMTYPES, 'cui': {v[0] for v in conso.values()}}, nrows=nrows)  # 80 MB
    t1 = time.time()
    print('Loading UMLS MRSTY.RRF to dictionary (s): ', t1 - t0)
    print('Size of UMLS MRSTY dictionary (B): ', sys.getsizeof(conso))

    return conso, sty


########################################################################
# Query test
########################################################################
# query = 'cancer'
# cuis = conso[cancer]
# stys = [sty[cui][0] for cui in conso[query] if sty[cui]]


# matcher = QuickUMLS('umls-2018-AA')
#
# use_list = False
# best_match = True
# ignore_syntax = False
#
#
# # with open('data/VA_My_HealtheVet_Blue_Button_Sample_Version_12_10.txt', 'r') as fd:
# with open('data/test.txt', 'r') as fd:
#     if use_list:
#         textlines = fd.readlines()
#         t0 = time.time()
#         for ln, text in enumerate(textlines):
#             matches = matcher.match(text, best_match=best_match, ignore_syntax=ignore_syntax)
#         t1 = time.time()
#     else:
#         text = fd.read().replace(os.linesep, '')
#         t0 = time.time()
#         # matches = matcher.match(text, best_match=best_match, ignore_syntax=ignore_syntax)
#         matches = matcher.match_mp(text, best_match=best_match, ignore_syntax=ignore_syntax)
#         t1 = time.time()
# print('Number of matches: ', len(matches))
# print('Time: ', t1 - t0)
