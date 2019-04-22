# future statements for Python 2 compatibility
from __future__ import (
    unicode_literals, division, print_function, absolute_import)

# built in modules
import os
import sys
import datetime
from six.moves import xrange

# installed modules
import spacy
import nltk
from unidecode import unidecode

# project modules
try:
    import toolbox
    import constants
except ImportError:
    from . import toolbox
    from . import constants


import cProfile
import math
import time
import itertools
import threading
import concurrent.futures
import multiprocessing.pool
import multiprocessing
import queue as Queue
NUM_THREADS = None
# NUM_THREADS = 4
PROFILE = False


class QuickUMLS(object):
    def __init__(
            self, quickumls_fp,
            overlapping_criteria='score', threshold=0.7, window=5,
            similarity_name='jaccard', min_match_length=3,
            accepted_semtypes=constants.ACCEPTED_SEMTYPES,
            verbose=False):

        self.verbose = verbose

        valid_criteria = {'length', 'score'}
        err_msg = (
            '"{}" is not a valid overlapping_criteria. Choose '
            'between {}'.format(
                overlapping_criteria, ', '.join(valid_criteria)
            )
        )
        assert overlapping_criteria in valid_criteria, err_msg
        self.overlapping_criteria = overlapping_criteria

        valid_similarities = {'dice', 'jaccard', 'cosine', 'overlap'}
        err_msg = ('"{}" is not a valid similarity name. Choose between '
                   '{}'.format(similarity_name, ', '.join(valid_similarities)))
        assert not(valid_similarities in valid_similarities), err_msg
        self.similarity_name = similarity_name

        simstring_fp = os.path.join(quickumls_fp, 'umls-simstring.db')
        cuisem_fp = os.path.join(quickumls_fp, 'cui-semtypes.db')

        self.valid_punct = constants.UNICODE_DASHES
        self.negations = constants.NEGATIONS

        self.window = window
        self.ngram_length = 3
        self.threshold = threshold
        self.min_match_length = min_match_length
        self.to_lowercase_flag = os.path.exists(
            os.path.join(quickumls_fp, 'lowercase.flag')
        )
        self.normalize_unicode_flag = os.path.exists(
            os.path.join(quickumls_fp, 'normalize-unicode.flag')
        )

        language_fp = os.path.join(quickumls_fp, 'language.flag')

        # download stopwords if necessary
        try:
            nltk.corpus.stopwords.words()
        except LookupError:
            nltk.download('stopwords')

        if os.path.exists(language_fp):
            with open(language_fp) as f:
                self.language_flag = f.read().strip()
        else:
            self.language_flag = 'ENG'

        if self.language_flag not in constants.LANGUAGES:
            raise ValueError('Language "{}" not supported'.format(self.language_flag))
        elif constants.LANGUAGES[self.language_flag] is None:
            self._stopwords = set()
            spacy_lang = 'XXX'
        else:
            self._stopwords = set(
                nltk.corpus.stopwords.words(constants.LANGUAGES[self.language_flag])
            )
            spacy_lang = constants.SPACY_LANGUAGE_MAP[self.language_flag]

        # domain specific stopwords
        self._stopwords = self._stopwords.union(constants.DOMAIN_SPECIFIC_STOPWORDS)

        self._info = None

        self.accepted_semtypes = accepted_semtypes

        self.ss_db = toolbox.SimstringDBReader(
            simstring_fp, similarity_name, threshold
        )
        self.cuisem_db = toolbox.CuiSemTypesDB(cuisem_fp)
        try:
            self.nlp = spacy.load(spacy_lang)
        except OSError:
            msg = (
                'Model for language "{}" is not downloaded. Please '
                'run "python -m spacy download {}" before launching '
                'QuickUMLS'
            ).format(
                self.language_flag,
                constants.SPACY_LANGUAGE_MAP.get(self.language_flag, 'xx')
            )
            raise OSError(msg)

    def get_info(self):
        return self.info

    def get_accepted_semtypes(self):
        return self.accepted_semtypes

    @property
    def info(self):
        # useful for caching of respnses

        if self._info is None:
            self._info = {
                'threshold': self.threshold,
                'similarity_name': self.similarity_name,
                'window': self.window,
                'ngram_length': self.ngram_length,
                'min_match_length': self.min_match_length,
                'accepted_semtypes': sorted(self.accepted_semtypes),
                'negations': sorted(self.negations),
                'valid_punct': sorted(self.valid_punct)
            }
        return self._info

    def _is_valid_token(self, tok):
        return not(
            tok.is_punct or tok.is_space or
            tok.pos_ == 'ADP' or tok.pos_ == 'DET' or tok.pos_ == 'CONJ'
        )

    def _is_valid_start_token(self, tok):
        return not(
            tok.like_num or
            (self._is_stop_term(tok) and tok.lemma_ not in self.negations) or
            tok.pos_ == 'ADP' or tok.pos_ == 'DET' or tok.pos_ == 'CONJ'
        )

    def _is_stop_term(self, tok):
        return tok.text in self._stopwords

    def _is_valid_end_token(self, tok):
        return not(
            tok.is_punct or tok.is_space or self._is_stop_term(tok) or
            tok.pos_ == 'ADP' or tok.pos_ == 'DET' or tok.pos_ == 'CONJ'
        )

    def _is_valid_middle_token(self, tok):
        return (
            not(tok.is_punct or tok.is_space) or
            tok.is_bracket or
            tok.text in self.valid_punct
        )

    def _is_ok_semtype(self, target_semtypes):
        if self.accepted_semtypes is None:
            ok = True
        else:
            ok = any(sem in self.accepted_semtypes for sem in target_semtypes)
        return ok

    def _is_longer_than_min(self, span):
        return (span.end_char - span.start_char) >= self.min_match_length

    def _make_batch_ngram1(self, *args):
        sent, start, chunk = args
        sent_length = len(sent)

        # Merge previous statements into a single loop
        skip_in_span = set()
        invalid_mid_tokens = set()
        for token in sent:
            if token.pos_ == 'DET':
                skip_in_span.add(token.i)
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        for i in range(start, min(sent_length, start + chunk)):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                yield (tok.idx, tok.idx + len(tok), tok.text)

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                yield (
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.i not in skip_in_span).strip()
                )


    def _make_batch_ngram2(self, *args):
        sent, start, chunk = args
        sent_length = len(sent)

        # Merge previous statements into a single loop
        skip_in_span = set()
        invalid_mid_tokens = set()
        for token in sent:
            if token.pos_ == 'DET':
                skip_in_span.add(token.i)
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        ngrams = []
        for i in range(start, min(sent_length, start + chunk)):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                ngrams.append((tok.idx, tok.idx + len(tok), tok.text))

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                ngrams.append((
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.i not in skip_in_span).strip()
                ))
        return ngrams


    def _make_ngrams_par1_batch(self, sent):
        ngrams = []
        with multiprocessing.pool.ThreadPool(processes=NUM_THREADS) as pool:
            handles = []
            batch_size = 2**6
            for i in range(0, len(sent), batch_size):
                handles.append(pool.apply_async(self._make_batch_ngram1, (sent, i, batch_size)))
            pool.close()
            for handle in handles:
                ngrams.extend(list(handle.get()))
        return ngrams


    def _make_ngrams_par2_batch(self, sent):
        ngrams = []
        with multiprocessing.pool.ThreadPool(processes=NUM_THREADS) as pool:
            handles = []
            batch_size = 2**6
            for i in range(0, len(sent), batch_size):
                handles.append(pool.apply_async(self._make_batch_ngram2, (sent, i, batch_size)))
            pool.close()
            for handle in handles:
                ngrams.extend(handle.get())
        return ngrams


    def _make_ngrams2(self, sent):
        sent_length = len(sent)

        # Merge previous statements into a single loop
        skip_in_span = set()
        invalid_mid_tokens = set()
        for token in sent:
            if token.pos_ == 'DET':
                skip_in_span.add(token.i)
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        for i in range(sent_length):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                yield (tok.idx, tok.idx + len(tok), tok.text)

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                yield (
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.i not in skip_in_span).strip()
                )


    def _make_ngrams(self, sent):
        sent_length = len(sent)

        # do not include teterminers inside a span
        skip_in_span = {token.i for token in sent if token.pos_ == 'DET'}

        # invalidate a span if it includes any on these  symbols
        invalid_mid_tokens = {
            token.i for token in sent if not self._is_valid_middle_token(token)
        }

        for i in xrange(sent_length):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                yield(tok.idx, tok.idx + len(tok), tok.text)

            for j in xrange(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                yield (
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.i not in skip_in_span).strip()
                )


    # par4
    def _get_batch_match(self, matches, args):
        for arg in args[0]:

    # par2
    # def _get_batch_match(self, args):
        # matches = []
        # for arg in args:

            start, end, ngram = arg

            ngram_normalized = ngram

            if self.normalize_unicode_flag:
                ngram_normalized = unidecode(ngram_normalized)

            # make it lowercase
            if self.to_lowercase_flag:
                ngram_normalized = ngram_normalized.lower()

            # if the term is all uppercase, it might be the case that
            # no match is found; so we convert to lowercase;
            # however, this is never needed if the string is lowercased
            # in the step above
            if not self.to_lowercase_flag and ngram_normalized.isupper():
                ngram_normalized = ngram_normalized.lower()

            prev_cui = None
            ngram_cands = list(self.ss_db.get(ngram_normalized))

            ngram_matches = []

            for match in ngram_cands:
                cuisem_match = sorted(self.cuisem_db.get(match))

                for cui, semtypes, preferred in cuisem_match:
                    match_similarity = toolbox.get_similarity(
                        x=ngram_normalized,
                        y=match,
                        n=self.ngram_length,
                        similarity_name=self.similarity_name
                    )
                    if match_similarity == 0:
                        continue

                    if not self._is_ok_semtype(semtypes):
                        continue

                    if prev_cui is not None and prev_cui == cui:
                        if match_similarity > ngram_matches[-1]['similarity']:
                            ngram_matches.pop(-1)
                        else:
                            continue

                    prev_cui = cui

                    ngram_matches.append(
                        {
                            'start': start,
                            'end': end,
                            'ngram': ngram,
                            'term': toolbox.safe_unicode(match),
                            'cui': cui,
                            'similarity': match_similarity,
                            'semtypes': semtypes,
                            'preferred': preferred
                        }
                    )

            if len(ngram_matches) > 0:
                matches.append(
                    sorted(
                        ngram_matches,
                        key=lambda m: m['similarity'] + m['preferred'],
                        reverse=True
                    )
                )
        return matches


    # par2
    # def _get_single_match(self, args):

    # default
    # par1
    # par3
    def _get_single_match(self, *args):

        start, end, ngram = args

        ngram_normalized = ngram

        if self.normalize_unicode_flag:
            ngram_normalized = unidecode(ngram_normalized)

        # make it lowercase
        if self.to_lowercase_flag:
            ngram_normalized = ngram_normalized.lower()

        # if the term is all uppercase, it might be the case that
        # no match is found; so we convert to lowercase;
        # however, this is never needed if the string is lowercased
        # in the step above
        if not self.to_lowercase_flag and ngram_normalized.isupper():
            ngram_normalized = ngram_normalized.lower()

        prev_cui = None
        ngram_cands = list(self.ss_db.get(ngram_normalized))

        ngram_matches = []

        for match in ngram_cands:
            cuisem_match = sorted(self.cuisem_db.get(match))

            for cui, semtypes, preferred in cuisem_match:
                match_similarity = toolbox.get_similarity(
                    x=ngram_normalized,
                    y=match,
                    n=self.ngram_length,
                    similarity_name=self.similarity_name
                )
                if match_similarity == 0:
                    continue

                if not self._is_ok_semtype(semtypes):
                    continue

                if prev_cui is not None and prev_cui == cui:
                    if match_similarity > ngram_matches[-1]['similarity']:
                        ngram_matches.pop(-1)
                    else:
                        continue

                prev_cui = cui

                ngram_matches.append(
                    {
                        'start': start,
                        'end': end,
                        'ngram': ngram,
                        'term': toolbox.safe_unicode(match),
                        'cui': cui,
                        'similarity': match_similarity,
                        'semtypes': semtypes,
                        'preferred': preferred
                    }
                )

        if len(ngram_matches) > 0:
            return  sorted(
                    ngram_matches,
                    key=lambda m: m['similarity'] + m['preferred'],
                    reverse=True
                )


    def _get_all_matches_par1(self, ngrams):
        matches = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            future_to_match = {executor.submit(self._get_single_match, *args): args for args in ngrams}
            for future in concurrent.futures.as_completed(future_to_match):
                data = future.result()
                if data is not None:
                    matches.append(data)
        return matches


    def _get_all_matches_par2(self, ngrams):
        matches = []
        with multiprocessing.pool.ThreadPool(processes=NUM_THREADS) as pool:
            # handle = pool.map_async(self._get_single_match, ngrams)
            # matches = list(filter(lambda x: x, handle.get()))
            matches = list(filter(lambda x: x, pool.map(self._get_single_match, ngrams)))
        return matches


    def _get_all_matches_par2_batch(self, ngrams):
        matches = []
        with multiprocessing.pool.ThreadPool(processes=NUM_THREADS) as pool:
            handles = []
            batch_size = 2**10
            for i in range(0, len(ngrams), batch_size):
                handles.append(pool.apply_async(self._get_batch_match, [ngrams[i:i+batch_size]]))
            pool.close()
            for handle in handles:
                matches.extend(handle.get())
        return matches


    def _get_all_matches_par3(self, ngrams):
        matches = []
        with multiprocessing.pool.ThreadPool(processes=NUM_THREADS) as pool:
            handles = []
            for ngram in ngrams:
                handles.append(pool.apply_async(self._get_single_match, ngram))
            pool.close()
            for handle in handles:
                data = handle.get()
                if data is not None:
                    matches.append(data)
        return matches


    def _get_all_matches_par4_batch(self, ngrams):
        matches = []
        handles = []
        batch_size = 2**10
        for i in range(0, len(ngrams), batch_size):
            handle = threading.Thread(target=self._get_batch_match, args=(matches, [ngrams[i:i+batch_size]]))
            handles.append(handle)
            handle.start()
        for handle in handles:
            handle.join()
        return matches


    def _get_all_matches(self, ngrams):
        matches = []
        for start, end, ngram in ngrams:
            ngram_normalized = ngram

            if self.normalize_unicode_flag:
                ngram_normalized = unidecode(ngram_normalized)

            # make it lowercase
            if self.to_lowercase_flag:
                ngram_normalized = ngram_normalized.lower()

            # if the term is all uppercase, it might be the case that
            # no match is found; so we convert to lowercase;
            # however, this is never needed if the string is lowercased
            # in the step above
            if not self.to_lowercase_flag and ngram_normalized.isupper():
                ngram_normalized = ngram_normalized.lower()

            # Access/retrieve from Simstring database
            prev_cui = None
            ngram_cands = list(self.ss_db.get(ngram_normalized))

            ngram_matches = []

            for match in ngram_cands:
                # Access/retrieve from CuiSem database
                cuisem_match = sorted(self.cuisem_db.get(match))

                for cui, semtypes, preferred in cuisem_match:
                    match_similarity = toolbox.get_similarity(
                        x=ngram_normalized,
                        y=match,
                        n=self.ngram_length,
                        similarity_name=self.similarity_name
                    )
                    if match_similarity == 0:
                        continue

                    if not self._is_ok_semtype(semtypes):
                        continue

                    if prev_cui is not None and prev_cui == cui:
                        if match_similarity > ngram_matches[-1]['similarity']:
                            ngram_matches.pop(-1)
                        else:
                            continue

                    prev_cui = cui

                    ngram_matches.append(
                        {
                            'start': start,
                            'end': end,
                            'ngram': ngram,
                            'term': toolbox.safe_unicode(match),
                            'cui': cui,
                            'similarity': match_similarity,
                            'semtypes': semtypes,
                            'preferred': preferred
                        }
                    )

            if len(ngram_matches) > 0:
                matches.append(
                    sorted(
                        ngram_matches,
                        key=lambda m: m['similarity'] + m['preferred'],
                        reverse=True
                    )
                )
        return matches

    @staticmethod
    def _select_score(match):
        return (match[0]['similarity'], (match[0]['end'] - match[0]['start']))

    @staticmethod
    def _select_longest(match):
        return ((match[0]['end'] - match[0]['start']), match[0]['similarity'])

    def _select_terms(self, matches):
        sort_func = (
            self._select_longest if self.overlapping_criteria == 'length'
            else self._select_score
        )

        matches = sorted(matches, key=sort_func, reverse=True)

        intervals = toolbox.Intervals()
        final_matches_subset = []

        for match in matches:
            match_interval = (match[0]['start'], match[0]['end'])
            if match_interval not in intervals:
                final_matches_subset.append(match)
                intervals.append(match_interval)

        return final_matches_subset

    def _make_token_sequences(self, parsed):
        for i in range(len(parsed)):
            for j in xrange(
                    i + 1, min(i + self.window, len(parsed)) + 1):
                span = parsed[i:j]

                if not self._is_longer_than_min(span):
                    continue

                yield (span.start_char, span.end_char, span.text)


    def _make_batch_token_sequence1(self, *args):
        parsed, start, chunk = args
        parsed_length = len(parsed)
        for i in range(start, min(parsed_length, start + chunk)):
            for j in range(i + 1, min(i + self.window, parsed_length) + 1):
                span = parsed[i:j]

                if not self._is_longer_than_min(span):
                    continue

                yield (span.start_char, span.end_char, span.text)

    def _make_batch_token_sequence2(self, *args):
        parsed, start, chunk = args
        parsed_length = len(parsed)
        ngrams = []
        for i in range(start, min(parsed_length, start + chunk)):
            for j in range(i + 1, min(i + self.window, parsed_length) + 1):
                span = parsed[i:j]

                if not self._is_longer_than_min(span):
                    continue

                ngrams.append((span.start_char, span.end_char, span.text))
        return ngrams


    def _make_token_sequences_par1_batch(self, parsed):
        ngrams = []
        with multiprocessing.pool.ThreadPool(processes=NUM_THREADS) as pool:
            handles = []
            batch_size = 2**6
            for i in range(0, len(parsed), batch_size):
                handles.append(pool.apply_async(self._make_batch_token_sequence1, (parsed, i, batch_size)))
            pool.close()
            for handle in handles:
                ngrams.extend(list(handle.get()))
        return ngrams

    def _make_token_sequences_par2_batch(self, parsed):
        ngrams = []
        with multiprocessing.pool.ThreadPool(processes=NUM_THREADS) as pool:
            handles = []
            batch_size = 2**6
            for i in range(0, len(parsed), batch_size):
                handles.append(pool.apply_async(self._make_batch_token_sequence2, (parsed, i, batch_size)))
            pool.close()
            for handle in handles:
                ngrams.extend(handle.get())
        return ngrams


    def _print_verbose_status(self, parsed, matches):
        if not self.verbose:
            return False

        print(
            '[{}] {:,} extracted from {:,} tokens'.format(
                datetime.datetime.now().isoformat(),
                sum(len(match_group) for match_group in matches),
                len(parsed)
            ),
            file=sys.stderr
        )
        return True

    def match(self, text, best_match=True, ignore_syntax=False):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        # Creates a spaCy Doc object
        # len(Doc) == number of words
        parsed = self.nlp(u'{}'.format(text))
        print('Num words: ', len(parsed))

        t0 = time.time()
        if ignore_syntax:
            ngrams = self._make_token_sequences(parsed)
            # ngrams = self._make_token_sequences_par1_batch(parsed)
            # ngrams = self._make_token_sequences_par2_batch(parsed)
        else:
            ngrams = self._make_ngrams(parsed)
            # ngrams = self._make_ngrams2(parsed)
            # ngrams = self._make_ngrams_par1_batch(parsed)
            # ngrams = self._make_ngrams_par2_batch(parsed)

        # Convert to list to enable batch processing
        if not isinstance(ngrams, list):
            ngrams = list(ngrams)
        t1 = time.time()
        print('Num ngrams: ', len(ngrams))
        print('_make_ngrams: ', t1 - t0, ' seconds')

        t0 = time.time()
        matches = self._get_all_matches(ngrams)
        # matches = self._get_all_matches_par1(ngrams)
        # matches = self._get_all_matches_par2(ngrams)
        # matches = self._get_all_matches_par2_batch(ngrams)
        # matches = self._get_all_matches_par3(ngrams)
        # matches = self._get_all_matches_par4_batch(ngrams)
        t1 = time.time()
        print('Num matches: ', len(matches))
        print('_get_all_matches: ', t1 - t0, ' seconds')

        t0 = time.time()
        if best_match:
            matches = self._select_terms(matches)
        t1 = time.time()
        print('Num best matches: ', len(matches))
        print('_select_terms: ', t1 - t0, ' seconds')

        self._print_verbose_status(parsed, matches)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return matches


    def match2(self, text, best_match=True, ignore_syntax=False):
        # Creates a spaCy Doc object
        # len(Doc) == number of words
        parsed = self.nlp(u'{}'.format(text))
        print('Num words: ', len(parsed))

        t0 = time.time()
        matches, ngrams = self._make_ngrams_get_all_matches(parsed)
        # matches = self._make_ngrams_get_all_matches2(parsed)
        t1 = time.time()
        print('Num matches: ', len(matches))
        print('_get_all_matches: ', t1 - t0, ' seconds')

        t0 = time.time()
        if best_match:
            matches = self._select_terms(matches)
        t1 = time.time()
        print('Num best matches: ', len(matches))
        print('_select_terms: ', t1 - t0, ' seconds')

        self._print_verbose_status(parsed, matches)

        return matches



    def _make_ngrams_get_all_matches(self, sent):
        matches = []
        ngrams = []
        with multiprocessing.pool.ThreadPool(processes=NUM_THREADS) as pool:
            handles = []
            batch_size = 2**6
            j = 0
            for i in range(0, len(sent), batch_size):
                matches.append([])
                ngrams.append([])
                # handles.append(pool.apply_async(self._make_batch_ngram_all_matches, (sent, i, batch_size)))
                handles.append(pool.apply_async(self._make_batch_ngram_all_matches, (matches[j], ngrams[j], sent, i, batch_size)))
                j += 1
            pool.close()
            # for handle in handles:
                # match, ngram = handle.get()
                # matches.extend(match)
                # ngrams.extend(ngram)
            pool.join()
            matches = list(itertools.chain.from_iterable(matches))
            ngrams = list(itertools.chain.from_iterable(ngrams))
        return matches, ngrams


    def _make_batch_ngram_all_matches(self, *args):
        matches, ngrams, sent, nstart, chunk = args
        sent_length = len(sent)

        # Merge previous statements into a single loop
        skip_in_span = set()
        invalid_mid_tokens = set()
        for token in sent:
            if token.pos_ == 'DET':
                skip_in_span.add(token.i)
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        for i in range(nstart, min(sent_length, nstart + chunk)):
            curr_ngrams = []
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                curr_ngrams.append((tok.idx, tok.idx + len(tok), tok.text))

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                curr_ngrams.append((
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.i not in skip_in_span).strip()
                ))

            ngrams.extend(curr_ngrams)

            # get_all_matches

            for start, end, ngram in curr_ngrams:
                ngram_normalized = ngram

                if self.normalize_unicode_flag:
                    ngram_normalized = unidecode(ngram_normalized)

                # make it lowercase
                if self.to_lowercase_flag:
                    ngram_normalized = ngram_normalized.lower()

                # if the term is all uppercase, it might be the case that
                # no match is found; so we convert to lowercase;
                # however, this is never needed if the string is lowercased
                # in the step above
                if not self.to_lowercase_flag and ngram_normalized.isupper():
                    ngram_normalized = ngram_normalized.lower()

                prev_cui = None
                ngram_cands = list(self.ss_db.get(ngram_normalized))

                ngram_matches = []

                for match in ngram_cands:
                    cuisem_match = sorted(self.cuisem_db.get(match))

                    for cui, semtypes, preferred in cuisem_match:
                        match_similarity = toolbox.get_similarity(
                            x=ngram_normalized,
                            y=match,
                            n=self.ngram_length,
                            similarity_name=self.similarity_name
                        )
                        if match_similarity == 0:
                            continue

                        if not self._is_ok_semtype(semtypes):
                            continue

                        if prev_cui is not None and prev_cui == cui:
                            if match_similarity > ngram_matches[-1]['similarity']:
                                ngram_matches.pop(-1)
                            else:
                                continue

                        prev_cui = cui

                        ngram_matches.append(
                            {
                                'start': start,
                                'end': end,
                                'ngram': ngram,
                                'term': toolbox.safe_unicode(match),
                                'cui': cui,
                                'similarity': match_similarity,
                                'semtypes': semtypes,
                                'preferred': preferred
                            }
                        )

                if len(ngram_matches) > 0:
                    matches.append(
                        sorted(
                            ngram_matches,
                            key=lambda m: m['similarity'] + m['preferred'],
                            reverse=True
                        )
                    )


    def _make_ngrams_get_all_matches2(self, sent):
        matches = []
        with multiprocessing.pool.ThreadPool(processes=NUM_THREADS) as pool:
            handles = []
            batch_size = 2**6
            for i in range(0, len(sent), batch_size):
                handles.append(pool.apply_async(self._get_all_matches_merge, (sent, i, batch_size)))
            pool.close()
            for handle in handles:
                # match = handle.get()
                match = list(handle.get())
                matches.extend(match)
        return matches


    def _make_batch_ngram_all_matches2(self, *args):
        sent, nstart, chunk = args
        sent_length = len(sent)

        # Merge previous statements into a single loop
        skip_in_span = set()
        invalid_mid_tokens = set()
        for token in sent:
            if token.pos_ == 'DET':
                skip_in_span.add(token.i)
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        ngrams = []
        matches = []
        for i in range(nstart, min(sent_length, nstart + chunk)):
            curr_ngrams = []
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                yield (tok.idx, tok.idx + len(tok), tok.text)

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                yield (
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.i not in skip_in_span).strip()
                )


    def _get_all_matches_merge(self, *args):
        matches = []
        for start, end, ngram in self._make_batch_ngram_all_matches2(*args):
            ngram_normalized = ngram

            if self.normalize_unicode_flag:
                ngram_normalized = unidecode(ngram_normalized)

            # make it lowercase
            if self.to_lowercase_flag:
                ngram_normalized = ngram_normalized.lower()

            # if the term is all uppercase, it might be the case that
            # no match is found; so we convert to lowercase;
            # however, this is never needed if the string is lowercased
            # in the step above
            if not self.to_lowercase_flag and ngram_normalized.isupper():
                ngram_normalized = ngram_normalized.lower()

            prev_cui = None
            ngram_cands = list(self.ss_db.get(ngram_normalized))

            ngram_matches = []

            for match in ngram_cands:
                cuisem_match = sorted(self.cuisem_db.get(match))

                for cui, semtypes, preferred in cuisem_match:
                    match_similarity = toolbox.get_similarity(
                        x=ngram_normalized,
                        y=match,
                        n=self.ngram_length,
                        similarity_name=self.similarity_name
                    )
                    if match_similarity == 0:
                        continue

                    if not self._is_ok_semtype(semtypes):
                        continue

                    if prev_cui is not None and prev_cui == cui:
                        if match_similarity > ngram_matches[-1]['similarity']:
                            ngram_matches.pop(-1)
                        else:
                            continue

                    prev_cui = cui

                    ngram_matches.append(
                        {
                            'start': start,
                            'end': end,
                            'ngram': ngram,
                            'term': toolbox.safe_unicode(match),
                            'cui': cui,
                            'similarity': match_similarity,
                            'semtypes': semtypes,
                            'preferred': preferred
                        }
                    )

            if len(ngram_matches) > 0:
                # matches.append(
                yield sorted(
                        ngram_matches,
                        key=lambda m: m['similarity'] + m['preferred'],
                        reverse=True
                    )
                # )
        return matches


    def match3(self, text, best_match=True, ignore_syntax=False):
        # Creates a spaCy Doc object
        # len(Doc) == number of words
        parsed = self.nlp(u'{}'.format(text))
        print('Num words: ', len(parsed))

        # Multiprocessing queue
        queue = multiprocessing.Queue()

        t0 = time.time()
        if ignore_syntax:
            ngrams = self._make_token_sequences(parsed)
        else:
            # ngrams = self._make_ngrams_mp(parsed, queue)
            # ngrams = self._make_ngrams_mp2(parsed, queue)
            ngrams = self._make_ngrams_mp3(parsed, queue)

        t1 = time.time()

        print('Num ngrams: ', len(ngrams))
        print('_make_ngrams: ', t1 - t0, ' seconds')

        t0 = time.time()
        matches = self._get_all_matches(ngrams)
        t1 = time.time()
        print('Num matches: ', len(matches))
        print('_get_all_matches: ', t1 - t0, ' seconds')

        t0 = time.time()
        if best_match:
            matches = self._select_terms(matches)
        t1 = time.time()
        print('Num best matches: ', len(matches))
        print('_select_terms: ', t1 - t0, ' seconds')

        self._print_verbose_status(parsed, matches)

        return matches


    def match4(self, text, best_match=True, ignore_syntax=False):
        # Creates a spaCy Doc object
        # len(Doc) == number of words
        parsed = self.nlp(u'{}'.format(text))
        print('Num words: ', len(parsed))

        t0 = time.time()
        matches = self._make_ngrams_mp4(parsed)
        t1 = time.time()

        print('Num matches: ', len(matches))
        print('_get_all_matches: ', t1 - t0, ' seconds')

        t0 = time.time()
        if best_match:
            matches = self._select_terms(matches)
        t1 = time.time()
        print('Num best matches: ', len(matches))
        print('_select_terms: ', t1 - t0, ' seconds')

        self._print_verbose_status(parsed, matches)

        return matches


    def _make_ngrams_mp(self, sent, queue):
        num_procs = multiprocessing.cpu_count() - 1
        batch_size = math.ceil(len(sent) / num_procs)
        print('Num processes: ', num_procs)

        handles = []
        for i in range(0, len(sent), batch_size):
            writer = multiprocessing.Process(target=self._make_batch_ngram_mp, args=((sent, i, batch_size, queue)))
            writer.daemon = True
            handles.append(writer)
            writer.start()

        ngrams = []
        proc_completed = 0
        while proc_completed < num_procs:
            data = queue.get()
            if data is None:
                proc_completed += 1
            else:
                ngrams.append(data)

        return ngrams


    def _make_ngrams_mp2(self, sent, queue):
        num_procs = multiprocessing.cpu_count() - 1
        batch_size = math.ceil(len(sent) / num_procs)
        print('Num processes: ', num_procs)

        bcount = 20
        handles = []
        for i in range(0, len(sent), batch_size):
            writer = multiprocessing.Process(target=self._make_batch_ngram_mp2, args=((sent, i, batch_size, queue, bcount)))
            writer.daemon = True
            writer.start()
            handles.append(writer)

        ngrams = []
        proc_completed = 0
        while proc_completed < num_procs:
            data = queue.get()
            if data is None:
                proc_completed += 1
            else:
                ngrams.extend(data)

        return ngrams


    def _make_ngrams_mp3(self, sent, queue):
        num_procs = multiprocessing.cpu_count() - 1
        batch_size = math.ceil(len(sent) / num_procs)
        print('Num processes: ', num_procs)

        skip_in_span = set()
        invalid_mid_tokens = set()
        for token in sent:
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        bcount = 100
        handles = []
        for i in range(0, len(sent), batch_size):
            writer = multiprocessing.Process(target=self._make_batch_ngram_mp3, args=((sent, i, batch_size, queue, bcount, invalid_mid_tokens)))
            writer.daemon = True
            writer.start()
            handles.append(writer)

        ngrams = []
        tasks_completed = 0
        while True:
            try:
                data = queue.get(False)
            except Queue.Empty as ex:
                print('Queue is empty')
                break
            else:
                ngrams.extend(data)
                tasks_completed += 1
        print('Tasks completed: ', tasks_completed)

        return ngrams


    def _make_ngrams_mp4(self, sent):
        num_procs = multiprocessing.cpu_count() - 1
        # num_procs = 1
        batch_size = math.ceil(len(sent) / num_procs)
        print('Num processes: ', num_procs)

        # Multiprocessing queues
        # iqueue - workers to database
        # oqueue - database to workers
        iqueue = multiprocessing.Queue()
        oqueue = multiprocessing.Queue()

        # Queue for final results
        matches_queue = multiprocessing.Queue()

        # Database process
        db = multiprocessing.Process(target=self.database_mp4, args=((iqueue, oqueue, num_procs)))
        db.daemon = True
        db.start()

        skip_in_span = set()
        invalid_mid_tokens = set()
        for token in sent:
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        # Worker processes
        for i in range(0, len(sent), batch_size):
            worker = multiprocessing.Process(target=self._make_batch_ngram_mp4, args=((sent, i, batch_size, iqueue, oqueue, matches_queue, invalid_mid_tokens)))
            worker.daemon = True
            worker.start()

        # Gather results from worker processes
        matches = []
        tasks_completed = 0
        done_procs = 0
        while True:
            data = matches_queue.get()
            # print('Match received: ', data)

            # Check if we are done
            if data is None:
                done_procs += 1
                if done_procs == num_procs:
                    break
                continue

            matches.extend(data)
            tasks_completed += 1
        print('Matches tasks completed: ', tasks_completed)

        return matches


    def _make_batch_ngram_mp(self, *args):
        sent, start, chunk, queue = args
        sent_length = len(sent)

        # Merge previous statements into a single loop
        skip_in_span = set()
        invalid_mid_tokens = set()
        for token in sent:
            if token.pos_ == 'DET':
                skip_in_span.add(token.i)
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        for i in range(start, min(sent_length, start + chunk)):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                queue.put((tok.idx, tok.idx + len(tok), tok.text))

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                queue.put_nowait((
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.i not in skip_in_span).strip()
                ))

        # Signal that we are done
        queue.put(None)


    def _make_batch_ngram_mp2(self, *args):
        sent, start, chunk, queue, bcount = args
        sent_length = len(sent)

        # Merge previous statements into a single loop
        skip_in_span = set()
        invalid_mid_tokens = set()
        for token in sent:
            if token.pos_ == 'DET':
                skip_in_span.add(token.i)
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        ngrams = []
        for i in range(start, min(sent_length, start + chunk)):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                ngrams.append((tok.idx, tok.idx + len(tok), tok.text))

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                ngrams.append((
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.i not in skip_in_span).strip()
                ))

            if len(ngrams) >= bcount:
                queue.put(ngrams)
                ngrams = []

        # Flush remaining ngrams
        if len(ngrams) > 0:
            queue.put(ngrams)

        # Signal that we are done
        queue.put(None)



    def _make_batch_ngram_mp3(self, *args):
        sent, start, chunk, queue, bcount, invalid_mid_tokens = args
        sent_length = len(sent)

        ngrams = []
        for i in range(start, min(sent_length, start + chunk)):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                ngrams.append((tok.idx, tok.idx + len(tok), tok.text))

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                ngrams.append((
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.pos_ != 'DET').strip()
                ))

            if len(ngrams) >= bcount:
                try:
                    queue.put_nowait(ngrams)
                except Queue.Full as ex:
                    queue.put(ngrams)
                ngrams = []

        # Flush remaining ngrams
        if len(ngrams) > 0:
            try:
                queue.put_nowait(ngrams)
            except Queue.Full as ex:
                queue.put(ngrams)


    def database_mp4(self, iqueue, oqueue, num_procs):

        tasks_completed = 0
        done_procs = 0
        while True:
            data = iqueue.get()

            # Check if we are done
            if data is None:
                done_procs += 1
                if done_procs == num_procs:
                    break
                continue

            # Receive a tuple of data to be processed along with metadata
            start, end, ngram, ngram_normalized = data

            # Access database and put results in dictionary
            # (ngram_cand, [cuisem_match])
            dct = []
            for ngram_cand in self.ss_db.get(ngram_normalized):
                dct.append((ngram_cand, sorted(self.cuisem_db.get(ngram_cand))))

            # Send tuple of metadata and dictionary
            # oqueue.put((start, end, ngram, ngram_normalized, dct))
            oqueue.put_nowait((start, end, ngram, ngram_normalized, dct))
            tasks_completed += 1
        print('Database completed tasks: ', tasks_completed)


    def _make_batch_ngram_mp4(self, *args):
        sent, start, chunk, iqueue, oqueue, matches_queue, invalid_mid_tokens = args
        sent_length = len(sent)

        total_ngrams = 0
        for i in range(start, min(sent_length, start + chunk)):
            ngrams = []
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                ngrams.append((tok.idx, tok.idx + len(tok), tok.text))

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                ngrams.append((
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.pos_ != 'DET').strip()
                ))

            # Skip if no ngrams found
            print(len(ngrams))
            if len(ngrams) == 0:
                continue
            total_ngrams += len(ngrams)

            for start, end, ngram in ngrams:
                ngram_normalized = ngram

                if self.normalize_unicode_flag:
                    ngram_normalized = unidecode(ngram_normalized)

                # make it lowercase
                if self.to_lowercase_flag:
                    ngram_normalized = ngram_normalized.lower()

                # if the term is all uppercase, it might be the case that
                # no match is found; so we convert to lowercase;
                # however, this is never needed if the string is lowercased
                # in the step above
                if not self.to_lowercase_flag and ngram_normalized.isupper():
                    ngram_normalized = ngram_normalized.lower()

                # Send ngram to database reader
                # iqueue.put((start, end, ngram, ngram_normalized))
                iqueue.put_nowait((start, end, ngram, ngram_normalized))

            # Process same amount sent to database
            for i in range(len(ngrams)):
                # Get results from database reader
                data = oqueue.get()
                start, end, ngram, ngram_normalized, dct = data

                matches = []
                ngram_matches = []
                prev_cui = None
                for match, cuisem_match in dct:
                    for cui, semtypes, preferred in cuisem_match:
                        match_similarity = toolbox.get_similarity(
                            x=ngram_normalized,
                            y=match,
                            n=self.ngram_length,
                            similarity_name=self.similarity_name
                        )
                        if match_similarity == 0:
                            continue

                        if not self._is_ok_semtype(semtypes):
                            continue

                        if prev_cui is not None and prev_cui == cui:
                            if match_similarity > ngram_matches[-1]['similarity']:
                                ngram_matches.pop(-1)
                            else:
                                continue

                        prev_cui = cui

                        ngram_matches.append(
                            {
                                'start': start,
                                'end': end,
                                'ngram': ngram,
                                'term': toolbox.safe_unicode(match),
                                'cui': cui,
                                'similarity': match_similarity,
                                'semtypes': semtypes,
                                'preferred': preferred
                            }
                        )

                if len(ngram_matches) > 0:
                    matches.append(
                        sorted(
                            ngram_matches,
                            key=lambda m: m['similarity'] + m['preferred'],
                            reverse=True
                        )
                    )

                    # Send matches results to main process
                    # matches_queue.put(matches)
                    matches_queue.put_nowait(matches)

        iqueue.put(None)
        matches_queue.put(None)


    def match5(self, text, best_match=True, ignore_syntax=False):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        # Creates a spaCy Doc object
        # len(Doc) == number of words
        parsed = self.nlp(u'{}'.format(text))
        print('Num words: ', len(parsed))

        t0 = time.time()
        matches = self._make_ngrams_mp5(parsed)
        t1 = time.time()

        print('Num matches: ', len(matches))
        print('_get_all_matches: ', t1 - t0, ' seconds')

        t0 = time.time()
        if best_match:
            matches = self._select_terms(matches)
        t1 = time.time()
        print('Num best matches: ', len(matches))
        print('_select_terms: ', t1 - t0, ' seconds')

        self._print_verbose_status(parsed, matches)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return matches


    def _make_ngrams_mp5(self, sent):
        requested_procs = multiprocessing.cpu_count() - 1
        # requested_procs = 1
        batch_size = math.ceil(len(sent) / requested_procs)
        num_procs = math.ceil(len(sent) / batch_size)
        print('Requested processes: ', requested_procs)
        print('Actual processes: ', num_procs)

        # Multiprocessing queues
        # iqueue - workers to database
        # oqueue - database to workers
        iqueue = multiprocessing.Queue()
        oqueue = multiprocessing.Queue()

        # Queue for final results
        matches_queue = multiprocessing.Queue()


        # Database process
        db = multiprocessing.Process(target=self.database_mp5, args=((iqueue, oqueue, num_procs)))
        db.daemon = True
        db.start()


        t0 = time.time()
        invalid_mid_tokens = set()
        for token in sent:
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)
        t1 = time.time()
        print('Time is_valid_middle_token: ', t1 - t0)

        # Worker processes
        for i in range(0, len(sent), batch_size):
            worker = multiprocessing.Process(target=self._make_batch_ngram_mp5, args=((sent, i, batch_size, iqueue, oqueue, matches_queue, invalid_mid_tokens)))
            worker.daemon = True
            worker.start()

        # Gather results from worker processes
        matches = []
        tasks_completed = 0
        done_procs = 0
        while True:
            data = matches_queue.get()
            # print('Match received: ', data)

            # Check if we are done
            if data is None:
                done_procs += 1
                if done_procs == num_procs:
                    break
                continue

            matches.extend(data)
            tasks_completed += 1
        print('Matches tasks completed: ', tasks_completed)

        return matches


    def database_mp5(self, iqueue, oqueue, num_procs):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        # Creates a spaCy Doc object
        tasks_completed = 0
        done_procs = 0
        while True:
            data = iqueue.get()

            # Check if we are done
            if data is None:
                done_procs += 1
                # print('(DB) Done process: ', done_procs)
                if done_procs == num_procs:
                    break
                continue

            # Receive a tuple of data to be processed along with metadata
            start, end, ngram, ngram_normalized = data

            # Access databases
            # (ngram_cand, (cuisem_matches))
            dct = [(ngram_cand, tuple(self.cuisem_db.get(ngram_cand)))
                   for ngram_cand in self.ss_db.get(ngram_normalized)]

            # Send tuple of metadata and dictionary
            # oqueue.put((start, end, ngram, ngram_normalized, dct))
            oqueue.put_nowait((start, end, ngram, ngram_normalized, dct))
            tasks_completed += 1
            # print('(DB) Matches completed: ', tasks_completed, ngram)
        print('(DB) Completed tasks: ', tasks_completed)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()


    def _make_batch_ngram_mp5(self, *args):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        sent, start, chunk, iqueue, oqueue, matches_queue, invalid_mid_tokens = args
        sent_length = len(sent)

        pid = math.ceil(start // chunk)
        total_ngrams = 0
        total_matches = 0
        for i in range(start, min(sent_length, start + chunk)):
            ngrams = []
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                ngrams.append((tok.idx, tok.idx + len(tok), tok.text))

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                ngrams.append((
                    span.start_char, span.end_char,
                    ''.join(token.text_with_ws for token in span
                            if token.pos_ != 'DET').strip()
                ))

            # Skip if no ngrams found
            if len(ngrams) == 0:
                # print(f'(W{pid}) Skipping, no ngrams')
                continue

            for start, end, ngram in ngrams:
                ngram_normalized = ngram

                if self.normalize_unicode_flag:
                    ngram_normalized = unidecode(ngram_normalized)

                # make it lowercase
                if self.to_lowercase_flag:
                    ngram_normalized = ngram_normalized.lower()

                # if the term is all uppercase, it might be the case that
                # no match is found; so we convert to lowercase;
                # however, this is never needed if the string is lowercased
                # in the step above
                if not self.to_lowercase_flag and ngram_normalized.isupper():
                    ngram_normalized = ngram_normalized.lower()

                # Send ngram to database reader
                # iqueue.put((start, end, ngram, ngram_normalized))
                iqueue.put_nowait((start, end, ngram, ngram_normalized))

            # print(f'(W{pid}) Attempt sent ngrams: ', len(ngrams), ngrams)
            total_ngrams += len(ngrams)

            # Get results from database reader
            try:
                data = oqueue.get_nowait()
            except:
                continue
            else:
                # print(f'(W{pid}) Received running matches: ', data)
                total_matches += self._get_all_matches_mp5(matches_queue, *data)
                continue

        # Process same number of ngrams
        while total_matches < total_ngrams:
            data = oqueue.get()
            # print(f'(W{pid}) Received end matches: ', data)
            total_matches += self._get_all_matches_mp5(matches_queue, *data)
            # print('(W) Total matches: ', total_matches)

        # print(f'W{pid} I am done')
        iqueue.put(None)
        matches_queue.put(None)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()


    def _get_all_matches_mp5(self, matches_queue, *args):
        start, end, ngram, ngram_normalized, dct = args
        matches = []
        ngram_matches = []
        prev_cui = None
        for match, cuisem_match in dct:
            cuisem_match = sorted(cuisem_match)
            for cui, semtypes, preferred in cuisem_match:
                match_similarity = toolbox.get_similarity(
                    x=ngram_normalized,
                    y=match,
                    n=self.ngram_length,
                    similarity_name=self.similarity_name
                )
                if match_similarity == 0:
                    continue

                if not self._is_ok_semtype(semtypes):
                    continue

                if prev_cui is not None and prev_cui == cui:
                    if match_similarity > ngram_matches[-1]['similarity']:
                        ngram_matches.pop(-1)
                    else:
                        continue

                prev_cui = cui

                ngram_matches.append(
                    {
                        'start': start,
                        'end': end,
                        'ngram': ngram,
                        'term': toolbox.safe_unicode(match),
                        'cui': cui,
                        'similarity': match_similarity,
                        'semtypes': semtypes,
                        'preferred': preferred
                    }
                )

        if len(ngram_matches) > 0:
            matches.append(
                sorted(
                    ngram_matches,
                    key=lambda m: m['similarity'] + m['preferred'],
                    reverse=True
                )
            )

            # Send matches results to main process
            # matches_queue.put(matches)
            matches_queue.put_nowait(matches)

        return 1



    def match6(self, text, best_match=True, ignore_syntax=False):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        # Creates a spaCy Doc object
        # len(Doc) == number of words
        parsed = self.nlp(u'{}'.format(text))
        print('Num words: ', len(parsed))

        t0 = time.time()
        matches = self._make_ngrams_mp6(parsed)
        t1 = time.time()

        print('Num matches: ', len(matches))
        print('_get_all_matches: ', t1 - t0, ' seconds')

        t0 = time.time()
        if best_match:
            matches = self._select_terms(matches)
        t1 = time.time()
        print('Num best matches: ', len(matches))
        print('_select_terms: ', t1 - t0, ' seconds')

        self._print_verbose_status(parsed, matches)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return matches


    def _make_ngrams_mp6(self, sent):
        requested_procs = multiprocessing.cpu_count() - 1
        # requested_procs = 1
        batch_size = math.ceil(len(sent) / requested_procs)
        num_procs = math.ceil(len(sent) / batch_size)
        print('Requested processes: ', requested_procs)
        print('Actual processes: ', num_procs)

        # Multiprocessing queues
        # iqueue - workers to database
        # oqueue - database to workers
        iqueue = multiprocessing.Queue()
        oqueue = multiprocessing.Queue()

        # Queue for final results
        matches_queue = multiprocessing.Queue()


        # Database process
        db = multiprocessing.Process(target=self.database_mp6, args=((iqueue, oqueue, num_procs)))
        db.daemon = True
        db.start()


        invalid_mid_tokens = set()
        for token in sent:
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        # Worker processes
        for pid, start in enumerate(range(0, len(sent), batch_size)):
            worker = multiprocessing.Process(target=self._make_batch_ngram_mp6, args=((sent, start, batch_size, pid, iqueue, oqueue, matches_queue, invalid_mid_tokens)))
            worker.daemon = True
            worker.start()

        # Gather results from worker processes
        matches = []
        tasks_completed = 0
        done_procs = 0
        while True:
            data = matches_queue.get()

            # Check if we are done
            if data is None:
                done_procs += 1
                if done_procs == num_procs:
                    break
                continue

            matches.extend(data)
            tasks_completed += 1
        print('Matches tasks completed: ', tasks_completed)

        return matches


    def database_mp6(self, iqueue, oqueue, num_procs):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        # Creates a spaCy Doc object
        tasks_completed = 0
        done_procs = 0
        while True:
            data = iqueue.get()

            # Check if we are done
            if data is None:
                print(f'(DB) Worker {done_procs} finished sending ngrams')
                done_procs += 1
                if done_procs == num_procs:
                    for _ in range(num_procs):
                        oqueue.put_nowait(None)
                    break
                continue

            # Receive a tuple of data to be processed along with metadata
            odata = []
            for start, end, ngram, ngram_normalized in data:
                # Access databases
                # (ngram_cand, (cuisem_matches))
                dct = [(ngram_cand, tuple(self.cuisem_db.get(ngram_cand)))
                       for ngram_cand in self.ss_db.get(ngram_normalized)]

                # Send tuple of metadata and dictionary
                # oqueue.put_nowait((start, end, ngram, ngram_normalized, dct))
                odata.append((start, end, ngram, ngram_normalized, dct))
                tasks_completed += 1
            oqueue.put_nowait(odata)
        print('(DB) Completed tasks: ', tasks_completed)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()


    def _make_batch_ngram_mp6(self, *args):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        sent, start, chunk, pid, iqueue, oqueue, matches_queue, invalid_mid_tokens = args
        sent_length = len(sent)

        at_least_ngrams = 1000
        sent_ngram = False
        total_ngrams = 0
        total_matches = 0
        ngrams = []
        for i in range(start, min(sent_length, start + chunk)):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_start_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of miminum length
            ):
                ngram_normalized = tok.text

                if self.normalize_unicode_flag:
                    ngram_normalized = unidecode(ngram_normalized)

                # make it lowercase
                if self.to_lowercase_flag:
                    ngram_normalized = ngram_normalized.lower()

                # if the term is all uppercase, it might be the case that
                # no match is found; so we convert to lowercase;
                # however, this is never needed if the string is lowercased
                # in the step above
                if not self.to_lowercase_flag and ngram_normalized.isupper():
                    ngram_normalized = ngram_normalized.lower()

                ngrams.append((tok.idx, tok.idx + len(tok), tok.text, ngram_normalized))

            for j in range(i + 1, span_end):
                if compensate:
                    compensate = False
                    continue

                if sent[j - 1] in invalid_mid_tokens:
                    break

                if not self._is_valid_end_token(sent[j - 1]):
                    continue

                span = sent[i:j]

                if not self._is_longer_than_min(span):
                    continue

                ngram = ''.join(token.text_with_ws for token in span
                            if token.pos_ != 'DET').strip()

                ngram_normalized = ngram

                if self.normalize_unicode_flag:
                    ngram_normalized = unidecode(ngram_normalized)

                # make it lowercase
                if self.to_lowercase_flag:
                    ngram_normalized = ngram_normalized.lower()

                # if the term is all uppercase, it might be the case that
                # no match is found; so we convert to lowercase;
                # however, this is never needed if the string is lowercased
                # in the step above
                if not self.to_lowercase_flag and ngram_normalized.isupper():
                    ngram_normalized = ngram_normalized.lower()

                ngrams.append((span.start_char, span.end_char, ngram, ngram_normalized))

            # Process batch
            if len(ngrams) < at_least_ngrams:
                # print(f'(W{pid}) Getting more ngrams')
                sent_ngrams = False
                continue

            # Send ngram to database reader
            try:
                iqueue.put_nowait(ngrams)
            except Queue.Full as ex:
                print(f'(W{pid}) Database queue is full')
                iqueue.put(ngrams)
            finally:
                sent_ngrams = True
                # print(f'(W{pid}) Attempt sent ngrams: ', len(ngrams))
                total_ngrams += len(ngrams)
                ngrams = []

            # Get results from database reader
            try:
                data = oqueue.get_nowait()
            except Queue.Empty as ex:
                # print(f'(W{pid}) No current result from database')
                continue
            else:
                # print(f'(W{pid}) Received match')
                for dat in data:
                    total_matches += self._get_all_matches_mp6(matches_queue, *dat)
                continue

        # Send ngram to database reader
        if not sent_ngrams:
            try:
                iqueue.put_nowait(ngrams)
            except Queue.Full as ex:
                print(f'(W{pid}) Database queue is full')
                iqueue.put(ngrams)
            finally:
                sent_ngrams = True
                total_ngrams += len(ngrams)

        print(f'W{pid} Total ngrams sent: ', total_ngrams)
        iqueue.put(None)

        # Process same number of ngrams
        # while total_matches < total_ngrams:
        while True:
            data = oqueue.get()
            if data is None:
                break
            # print(f'(W{pid}) Received match')
            for dat in data:
                total_matches += self._get_all_matches_mp6(matches_queue, *dat)
        print(f'(W{pid}) Total matches received: ', total_matches)

        matches_queue.put(None)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()


    def _get_all_matches_mp6(self, matches_queue, *args):
        start, end, ngram, ngram_normalized, dct = args
        matches = []
        ngram_matches = []
        prev_cui = None
        for match, cuisem_match in dct:
            cuisem_match = sorted(cuisem_match)
            for cui, semtypes, preferred in cuisem_match:
                match_similarity = toolbox.get_similarity(
                    x=ngram_normalized,
                    y=match,
                    n=self.ngram_length,
                    similarity_name=self.similarity_name
                )
                if match_similarity == 0:
                    continue

                if not self._is_ok_semtype(semtypes):
                    continue

                if prev_cui is not None and prev_cui == cui:
                    if match_similarity > ngram_matches[-1]['similarity']:
                        ngram_matches.pop(-1)
                    else:
                        continue

                prev_cui = cui

                ngram_matches.append(
                    {
                        'start': start,
                        'end': end,
                        'ngram': ngram,
                        'term': toolbox.safe_unicode(match),
                        'cui': cui,
                        'similarity': match_similarity,
                        'semtypes': semtypes,
                        'preferred': preferred
                    }
                )

        if len(ngram_matches) > 0:
            matches.append(
                sorted(
                    ngram_matches,
                    key=lambda m: m['similarity'] + m['preferred'],
                    reverse=True
                )
            )

            # Send matches results to main process
            # matches_queue.put(matches)
            matches_queue.put_nowait(matches)

        return 1
