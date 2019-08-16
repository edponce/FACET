import os
import sys
import math
import time
import spacy
import numpy
import datetime
import itertools
import threading
import collections
import queue as Queue
import multiprocessing
import concurrent.futures
import multiprocessing.pool
from unidecode import unidecode
from typing import List, Dict, Any, Union, Tuple, Iterable

from QuickerUMLS.file_system import unpack_dir, corpus_generator

from QuickerUMLS.umls_constants import (
    LANGUAGES,
    ACCEPTED_SEMTYPES,
)

from QuickerUMLS.toolbox import (
    UMLSDB,
    SimstringDBReader,
    safe_unicode,
)


# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


# 0 = No status info
# 1 = Processing rate
# 2 = Data structures info
VERBOSE = 1

# Number of threads/processes
NUM_THREADS = None

SIMSTRING_DB = 'umls-simstring.db'
LEVEL_DB = 'cui-semtypes.db'


class Intervals:
    def __init__(self):
        self.intervals = []

    def _is_overlapping_intervals(self, a, b):
        if b[0] < a[1] and b[1] > a[0]:
            return True
        elif a[0] < b[1] and a[1] > b[0]:
            return True
        else:
            return False

    def __contains__(self, interval):
        return any(
            self._is_overlapping_intervals(interval, other)
            for other in self.intervals
        )

    def append(self, interval):
        self.intervals.append(interval)


class QuickUMLS:
    NEGATIONS = {
        'none', 'non', 'neither', 'nor', 'no', 'not'
    }

    UNICODE_DASHES = {
        u'\u002d', u'\u007e', u'\u00ad', u'\u058a', u'\u05be', u'\u1400',
        u'\u1806', u'\u2010', u'\u2011', u'\u2010', u'\u2012', u'\u2013',
        u'\u2014', u'\u2015', u'\u2053', u'\u207b', u'\u2212', u'\u208b',
        u'\u2212', u'\u2212', u'\u2e17', u'\u2e3a', u'\u2e3b', u'\u301c',
        u'\u3030', u'\u30a0', u'\ufe31', u'\ufe32', u'\ufe58', u'\ufe63',
        u'\uff0d'
    }

    def __init__(self, db_dir, **kwargs):
        """
        Args:
            db_dir (str): Directory of installed database.

            overlap_criteria (str): Criteria for measuring string
                overlaps. Valid values are: 'length' and 'score'.
                Default is 'score'.

            measure (str): Name of similarity measure.
                Valid values are: 'exact', 'dice', 'jaccard', 'cosine', and
                'overlap'. Default is 'cosine'.

            threshold (float): Threshold value for similarity measure.
                Valid values are: (0., 1.]. Default value is 0.7.

            window (int): Window size for processing words.
                Default value is 5.

            min_match_length (int): Minimum number of characters for matching
                criterion. Default value is 3.

            ngram_length (int): Length of N-grams. Default value is 3.

            language (str): Language. Default is 'ENG'.

            stopwords (Iterable[str]): Domain specific stopwords. These are
                added to spaCy's language-specific stopwords.
        """
        # NOTE: db_dir, threshold, language are not needed as class variables,
        # but are used for printing configuration.
        self.db_dir = db_dir
        self.overlap_criteria = kwargs.get('overlap_criteria', 'score')
        self.measure = kwargs.get('measure', 'jaccard')
        self.threshold = kwargs.get('threshold', 0.7)
        self.window = kwargs.get('window', 5)
        self.min_match_length = kwargs.get('min_match_length', 3)
        self.ngram_length = kwargs.get('ngram_length', 3)
        self.language = kwargs.get('language', 'ENG')

        t1 = time.time()
        # TODO: Parameterize and generalize these files.
        # Also, check that files exist and have valid access.
        self.ss_db = SimstringDBReader(
            os.path.join(db_dir, SIMSTRING_DB),
            measure=self.measure,
            threshold=self.threshold
        )
        print(f'Loading Simstring: {time.time() - t1} sec')

        t1 = time.time()
        self.cuisem_db = UMLSDB(os.path.join(db_dir, LEVEL_DB))
        print(f'Loading UMLS-LevelDB: {time.time() - t1} sec')

        t1 = time.time()
        try:
            self.nlp = spacy.load(LANGUAGES[self.language])
        except KeyError:
            err = (f"Model for language '{self.language}' is not valid.")
            raise KeyError(err)
        except OSError as ex:
            err = (f"Please run 'python -m spacy download {self.language}'")
            raise OSError(ex)
        # Combine spaCy stopwords with domain specific stopwords
        self.nlp.Defaults.stop_words.update(kwargs.get('stopwords', ()))
        print(f'Loading Spacy model: {time.time() - t1} sec')

        # NOTE: Save database installation settings into a JSON configuration
        # file, which can be loaded by applications using the database. This
        # approach is better than having these sentinel files.
        self.lowercase = os.path.exists(
            os.path.join(db_dir, 'lowercase.flag'))
        self.normalize_unicode = os.path.exists(
            os.path.join(db_dir, 'normalize-unicode.flag'))

        if VERBOSE > 0:
            print(self.info())

    def __del__(self):
        self.ss_db.close()
        self.cuisem_db.close()

    def info(self):
        return {
            'database': self.db_dir,
            'overlap_criteria': self.overlap_criteria,
            'measure': self.measure,
            'threshold': self.threshold,
            'window': self.window,
            'min_match_length': self.min_match_length,
            'ngram_length': self.ngram_length,
            'language': self.language
        }

    def get_similarity(self, x, y, n):
        def make_ngrams(s, n):
            n = min(len(s), n)
            return set(s[i:i + n] for i in range(len(s) - n + 1))

        # Similarity between one or more empty strings is 0.
        if len(x) == 0 or len(y) == 0:
            return 0.

        X = make_ngrams(x, n)
        Y = make_ngrams(y, n)
        intersec = len(X.intersection(Y))

        if self.measure == 'dice':
            return 2 * intersec / (len(X) + len(Y))
        elif self.measure == 'jaccard':
            return intersec / (len(X) + len(Y) - intersec)
        elif self.measure == 'cosine':
            return intersec / numpy.sqrt(len(X) * len(Y))
        elif self.measure == 'overlap':
            return float(intersec)
        elif self.measure == 'exact':
            return float(X == Y)
        else:
            return 0.

    def _is_valid_token(self, tok):
        return not(
            tok.is_punct
            or tok.is_space
            or tok.pos_ == 'ADP'
            or tok.pos_ == 'DET'
            or tok.pos_ == 'CONJ'
        )

    def _is_valid_begin_token(self, tok):
        return not(
            tok.like_num
            or (self._is_stop_term(tok)
                and tok.lemma_ not in type(self).NEGATIONS)
            or tok.pos_ == 'ADP'
            or tok.pos_ == 'DET'
            or tok.pos_ == 'CONJ'
        )

    def _is_stop_term(self, tok):
        return tok.text in self.nlp.Defaults.stop_words

    def _is_valid_end_token(self, tok):
        return not(
            tok.is_punct
            or tok.is_space
            or self._is_stop_term(tok)
            or tok.pos_ == 'ADP'
            or tok.pos_ == 'DET'
            or tok.pos_ == 'CONJ'
        )

    def _is_valid_middle_token(self, tok):
        return (
            not(tok.is_punct or tok.is_space)
            or tok.is_bracket
            or tok.text in type(self).UNICODE_DASHES
        )

    def _is_ok_semtype(self, target_semtypes):
        if ACCEPTED_SEMTYPES is None:
            ok = True
        else:
            if isinstance(target_semtypes, str):
                ok = target_semtypes in ACCEPTED_SEMTYPES
            else:
                ok = any(sem in ACCEPTED_SEMTYPES for sem in target_semtypes)
        return ok

    def _is_longer_than_min(self, span):
        return (span.end_char - span.start_char) >= self.min_match_length

    def _make_ngrams(self, sent) -> Tuple[Any]:
        sent_length = len(sent)

        # do not include determiners inside a span
        skip_in_span = {token.i for token in sent if token.pos_ == 'DET'}

        # invalidate a span if it includes any on these symbols
        invalid_mid_tokens = {
            token.i for token in sent if not self._is_valid_middle_token(token)
        }

        for i in range(sent_length):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_begin_token(tok):
                compensate = False
            else:
                compensate = True

            span_end = min(sent_length, i + self.window) + 1

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == sent_length and            # it's the last token
                self._is_valid_end_token(tok) and   # it's a valid end token
                len(tok) >= self.min_match_length   # it's of minimum length
            ):
                yield(tok.idx, tok.idx + len(tok), tok.text)

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

    def _get_all_matches2(self, ngrams: Iterable[Any]) -> List[Dict[str, Any]]:
        matches = []
        for begin, end, ngram in ngrams:
            ngram_normalized = ngram

            if self.normalize_unicode:
                ngram_normalized = unidecode(ngram_normalized)

            if self.lowercase:
                ngram_normalized = ngram_normalized.lower()
            elif ngram_normalized.isupper():
                # if the term is all uppercase, it might be the case that
                # no match is found; so we convert to lowercase;
                ngram_normalized = ngram_normalized.lower()

            # Do not use Simstring database
            ngram_candidates = [ngram_normalized]
            ngram_matches = []
            prev_cui = None
            for candidate in ngram_candidates:
                similarity = self.get_similarity(
                    x=ngram_normalized,
                    y=candidate,
                    n=self.ngram_length,
                )

                if similarity <= 0.:
                    continue

                # Access/retrieve from CuiSem database
                cuisem_match = sorted(self.cuisem_db.read(candidate))
                for cui, semtypes, preferred in cuisem_match:
                    if not self._is_ok_semtype(semtypes):
                        continue

                    # Remove previously matched CUI if current match
                    # is more similar.
                    # Requires matches to be ordered by CUIs.
                    if prev_cui is not None and prev_cui == cui:
                        if similarity > ngram_matches[-1]['similarity']:
                            ngram_matches.pop(-1)
                        else:
                            continue

                    prev_cui = cui
                    ngram_matches.append({
                        'begin': begin,
                        'end': end,
                        'ngram': ngram,
                        'term': safe_unicode(candidate),
                        'cui': cui,
                        'similarity': similarity,
                        'semantic_types': semtypes,
                        'preferred': preferred
                    })

                # Sort matches by similarity and preference
                if len(ngram_matches) > 0:
                    matches.append(
                        sorted(
                            ngram_matches,
                            key=lambda m: m['similarity'] + m['preferred'],
                            reverse=True
                        )
                    )
        return matches

    def _get_all_matches(self, ngrams: Iterable[Any]) -> List[Dict[str, Any]]:
        matches = []
        for begin, end, ngram in ngrams:
            ngram_normalized = ngram

            if self.normalize_unicode:
                ngram_normalized = unidecode(ngram_normalized)

            if self.lowercase:
                ngram_normalized = ngram_normalized.lower()
            elif ngram_normalized.isupper():
                # if the term is all uppercase, it might be the case that
                # no match is found; so we convert to lowercase;
                ngram_normalized = ngram_normalized.lower()

            # Access/retrieve from Simstring database
            ngram_candidates = list(self.ss_db.read(ngram_normalized))
            ngram_matches = []
            prev_cui = None
            for candidate in ngram_candidates:
                similarity = self.get_similarity(
                    x=ngram_normalized,
                    y=candidate,
                    n=self.ngram_length,
                )

                if similarity <= 0.:
                    continue

                # Access/retrieve from CuiSem database
                cuisem_match = sorted(self.cuisem_db.read(candidate))
                for cui, semtypes, preferred in cuisem_match:
                    if not self._is_ok_semtype(semtypes):
                        continue

                    # Remove previously matched CUI if current match
                    # is more similar.
                    # Requires matches to be ordered by CUIs.
                    if prev_cui is not None and prev_cui == cui:
                        if similarity > ngram_matches[-1]['similarity']:
                            ngram_matches.pop(-1)
                        else:
                            continue

                    prev_cui = cui
                    ngram_matches.append({
                        'begin': begin,
                        'end': end,
                        'ngram': ngram,
                        'term': safe_unicode(candidate),
                        'cui': cui,
                        'similarity': similarity,
                        'semantic_types': semtypes,
                        'preferred': preferred
                    })

            # Sort matches by similarity and preference
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
        return (match[0]['similarity'], (match[0]['end'] - match[0]['begin']))

    @staticmethod
    def _select_longest(match):
        return ((match[0]['end'] - match[0]['begin']), match[0]['similarity'])

    def _select_terms(self, matches):
        if self.overlap_criteria == 'length':
            sort_func = self._select_longest
        elif self.overlap_criteria == 'score':
            sort_func = self._select_score
        matches = sorted(matches, key=sort_func, reverse=True)

        intervals = Intervals()
        final_matches_subset = []
        for match in matches:
            match_interval = (match[0]['begin'], match[0]['end'])
            if match_interval not in intervals:
                final_matches_subset.append(match)
                intervals.append(match_interval)

        return final_matches_subset

    def _make_token_sequences(self, parsed):
        for i in range(len(parsed)):
            for j in range(
                    i + 1, min(i + self.window, len(parsed)) + 1):
                span = parsed[i:j]

                if not self._is_longer_than_min(span):
                    continue

                yield (span.start_char, span.end_char, span.text)

    def _print_status(self, parsed, matches):
        print(
            '[{}] {:,} extracted from {:,} tokens'.format(
                datetime.datetime.now().isoformat(),
                sum(len(match_group) for match_group in matches),
                len(parsed)
            ),
            file=sys.stdout
        )

    def match(self,
              corpora,
              best_match=True,
              ignore_syntax=False,
              **kwargs) -> Dict[str, List[List[Dict[str, Any]]]]:
        """
        Example:
        >>> matches = QuickUMLS.match(['file1.txt', 'file2.txt', ...])
        >>> for terms in matches['file1.txt']:
        >>>     for term in terms:
        >>>         print(term['term'], term['cui'], term['semantic_types'])
        """
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        matches = collections.defaultdict(list)
        for source, corpus in corpus_generator(corpora, **kwargs):
            # Creates a spaCy Doc object
            # len(Doc) == number of words
            t0 = time.time()
            doc = self.nlp.make_doc(corpus)
            t1 = time.time()
            print('Num words: ', len(doc))
            print('spaCy parse: ', t1 - t0, ' seconds')

            t0 = time.time()
            if ignore_syntax:
                ngrams = self._make_token_sequences(doc)
            else:
                ngrams = self._make_ngrams(doc)
            t1 = time.time()
            print('_make_ngrams: ', t1 - t0, ' seconds')

            t0 = time.time()
            _matches = self._get_all_matches(ngrams)
            # _matches = self._get_all_matches2(ngrams)
            t1 = time.time()
            print('Num matches: ', len(_matches))
            print('_get_all_matches: ', t1 - t0, ' seconds')

            t0 = time.time()
            if best_match:
                _matches = self._select_terms(_matches)
            t1 = time.time()
            print('Num best matches: ', len(_matches))
            print('_select_terms: ', t1 - t0, ' seconds')

            if VERBOSE > 0:
                self._print_status(doc, _matches)

            matches[source].extend(_matches)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return matches

    def match_mp(self, text, best_match=True, ignore_syntax=False):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        # Creates a spaCy Doc object
        # len(Doc) == number of words
        t0 = time.time()
        parsed = self.nlp(u'{}'.format(text))
        t1 = time.time()
        print('Num words: ', len(parsed))
        print('spaCy parse: ', t1 - t0, ' seconds')

        t0 = time.time()
        matches = self._make_ngrams_mp(parsed)
        t1 = time.time()
        print('Num matches: ', len(matches))
        print('_get_all_matches: ', t1 - t0, ' seconds')

        t0 = time.time()
        if best_match:
            matches = self._select_terms(matches)
        t1 = time.time()
        print('Num best matches: ', len(matches))
        print('_select_terms: ', t1 - t0, ' seconds')

        if VERBOSE > 0:
            self._print_status(parsed, matches)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return matches

    def _make_ngrams_mp(self, sent) -> Tuple[Any]:
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
        db = multiprocessing.Process(
            target=self.database_mp,
            args=((
                iqueue,
                oqueue,
                num_procs
            ))
        )
        db.daemon = True
        db.start()

        invalid_mid_tokens = set()
        for token in sent:
            if not self._is_valid_middle_token(token):
                invalid_mid_tokens.add(token.i)

        # Worker processes
        for pid, begin in enumerate(range(0, len(sent), batch_size)):
            worker = multiprocessing.Process(
                target=self._make_batch_ngram_mp,
                args=((
                    sent,
                    begin,
                    batch_size,
                    pid,
                    iqueue,
                    oqueue,
                    matches_queue,
                    invalid_mid_tokens
                ))
            )
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

    def database_mp(self, iqueue, oqueue, num_procs):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

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
            for begin, end, ngram, ngram_normalized in data:
                # Access databases
                # (ngram_cand, (cuisem_matches))
                dct = [(ngram_cand, tuple(self.cuisem_db.read(ngram_cand)))
                       for ngram_cand in self.ss_db.read(ngram_normalized)]

                # Send tuple of metadata and dictionary
                # oqueue.put_nowait((begin, end, ngram, ngram_normalized, dct))
                odata.append((begin, end, ngram, ngram_normalized, dct))
                tasks_completed += 1
            oqueue.put_nowait(odata)
        print('(DB) Completed tasks: ', tasks_completed)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

    def _make_batch_ngram_mp(self, *args):
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        sent, begin, chunk, pid, iqueue, oqueue, matches_queue, \
            invalid_mid_tokens = args
        sent_length = len(sent)

        at_least_ngrams = 20
        sent_ngrams = False
        total_ngrams = 0
        total_matches = 0
        ngrams = []
        for i in range(begin, min(sent_length, begin + chunk)):
            tok = sent[i]

            if not self._is_valid_token(tok):
                continue

            # do not consider this token by itself if it is
            # a number or a stopword.
            if self._is_valid_begin_token(tok):
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

                if self.normalize_unicode:
                    ngram_normalized = unidecode(ngram_normalized)

                # make it lowercase
                if self.lowercase:
                    ngram_normalized = ngram_normalized.lower()

                # if the term is all uppercase, it might be the case that
                # no match is found; so we convert to lowercase;
                # however, this is never needed if the string is lowercased
                # in the step above
                if not self.lowercase and ngram_normalized.isupper():
                    ngram_normalized = ngram_normalized.lower()

                ngrams.append(
                    (tok.idx, tok.idx + len(tok), tok.text, ngram_normalized)
                )

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

                if self.normalize_unicode:
                    ngram_normalized = unidecode(ngram_normalized)

                # make it lowercase
                if self.lowercase:
                    ngram_normalized = ngram_normalized.lower()
                elif gram_normalized.isupper():
                    # if the term is all uppercase, it might be the case that
                    # no match is found; so we convert to lowercase;
                    ngram_normalized = ngram_normalized.lower()

                ngrams.append(
                    (span.start_char, span.end_char, ngram, ngram_normalized)
                )

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
                    total_matches += self._get_all_matches_mp(matches_queue,
                                                              *dat)
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
                total_matches += self._get_all_matches_mp(matches_queue, *dat)
        print(f'(W{pid}) Total matches received: ', total_matches)

        matches_queue.put(None)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

    def _get_all_matches_mp(self, matches_queue, *args):
        begin, end, ngram, ngram_normalized, dct = args
        matches = []
        ngram_matches = []
        prev_cui = None
        for match, cuisem_match in dct:
            similarity = self.get_similarity(
                x=ngram_normalized,
                y=match,
                n=self.ngram_length,
            )

            if similarity == 0:
                continue

            cuisem_match = sorted(cuisem_match)
            for cui, semtypes, preferred in cuisem_match:
                if not self._is_ok_semtype(semtypes):
                    continue

                if prev_cui is not None and prev_cui == cui:
                    if similarity > ngram_matches[-1]['similarity']:
                        ngram_matches.pop(-1)
                    else:
                        continue

                prev_cui = cui
                ngram_matches.append({
                    'begin': begin,
                    'end': end,
                    'ngram': ngram,
                    'term': safe_unicode(match),
                    'cui': cui,
                    'similarity': similarity,
                    'semantic_types': semtypes,
                    'preferred': preferred
                })

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
