import os
import time
import spacy


UNICODE_DASHES = {
    u'\u002d', u'\u007e', u'\u00ad', u'\u058a', u'\u05be', u'\u1400',
    u'\u1806', u'\u2010', u'\u2011', u'\u2010', u'\u2012', u'\u2013',
    u'\u2014', u'\u2015', u'\u2053', u'\u207b', u'\u2212', u'\u208b',
    u'\u2212', u'\u2212', u'\u2e17', u'\u2e3a', u'\u2e3b', u'\u301c',
    u'\u3030', u'\u30a0', u'\ufe31', u'\ufe32', u'\ufe58', u'\ufe63',
    u'\uff0d'
}


def is_valid_middle_token(tok):
    return (
        not(tok.is_punct or tok.is_space) or
        tok.is_bracket or
        tok.text in UNICODE_DASHES
    )



with open('data/VA_My_HealtheVet_Blue_Button_Sample_Version_12_10.txt', 'r') as fd:
    text = fd.read().replace(os.linesep, '')

nlp = spacy.load('en')
# doc = nlp(text)
doc = nlp(text * 5)


# 0.012 (text)
# 0.06 (text * 5)
t0 = time.time()
skip_in_span = {token.i for token in doc if token.pos_ == 'DET'}
invalid_mid_tokens = {
    token.i for token in doc if not is_valid_middle_token(token)
}
t1 = time.time()
print('Time: ', t1 - t0)


# 0.011 (text)
# 0.05  (text * 5)
t0 = time.time()
skip_in_span = set()
invalid_mid_tokens = set()
for token in doc:
    if token.pos_ == 'DET':
        skip_in_span.add(token.i)
    if not is_valid_middle_token(token):
        invalid_mid_tokens.add(token.i)
t1 = time.time()
print('Time: ', t1 - t0)
