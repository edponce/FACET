import os
import sys
import csv
import time
import json
import spacy
import pickle
import dicttoxml
import collections
import xml.dom.minidom
from unidecode import unidecode
from QuickerUMLS.simstring import Simstring
from QuickerUMLS.database import DictDatabase
from QuickerUMLS.helpers import (
    # data_to_dict,
    iter_data,
    is_iterable,
    unpack_dir,
    corpus_generator,
)
from QuickerUMLS.umls_constants import (
    HEADERS_MRCONSO,
    HEADERS_MRSTY,
    ACCEPTED_SEMTYPES,
)
from typing import (
    Any,
    Union,
    List,
    Dict,
    Tuple,
    Callable,
    Iterable,
    Generator,
    NoReturn,
)


__all__ = ['FACET']


VERBOSE = True

# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


class Installer:
    """FACET installation tool.

    Kwargs:
        conso_db (BaseDatabase): Handle to database instance for CONCEPT-CUI
            storage.

        cuisty_db (BaseDatabase): Handle to database instance for CUI-STY
            storage.

        simstring (Simstring): Handle to Simstring instance.
            The database handle should differ from the internal Simstring
            database handle (to prevent collisions with N-grams).
    """

    def __init__(self, **kwargs):
        self._conso_db = kwargs.get('conso_db', DictDatabase())
        self._cuisty_db = kwargs.get('cuisty_db', DictDatabase())
        self._ss = kwargs.get('simstring', Simstring())

    def _load_conso(
        self,
        afile: str,
        *,
        language: Union[str, Iterable[str]] = ['ENG'],
        nrows: Union[int, None] = None,
    ) -> 'Generator':
        """
        Args:
            afile (str): File with contents to load.

            language (Union[str, Iterable[str]]): Extract concepts of the
                specified languages. Default is 'ENG'.

            nrows (Union[int, None]): Maximum number of records to load
                from file. Default is None (all records).
        """
        def configure_converters() -> Dict[str, List[Callable]]:
            """Converter functions are used to process dataset during load
            operations.

            Converter functions should only take a single parameter
            (or use lambda and preset extra parameters).

            Returns (Dict[str, List[Callable]]): Mapping of column IDs
                to list of functions.
            """
            converters = collections.defaultdict(list)
            converters['str'].append(str.lower)
            converters['str'].append(unidecode)
            converters['ispref'].append(lambda v: True if v == 'Y' else False)
            return converters

        if not is_iterable(language):
            language = [language]

        # NOTE: This version returns a dictionary data structure
        # which is necessary if semantic types are going to be used as a
        # filter for CUIs. See NOTE in loading of semantic types.
        # return data_to_dict(
        return iter_data(
            afile,
            ['str', 'cui'],  # key
            ['ispref'],      # values
            headers=HEADERS_MRCONSO,
            valids={'lat': language},
            converters=configure_converters(),
            unique_keys=True,
            nrows=nrows,
        )

    def _load_sty(
        self,
        afile: str,
        *,
        conso: Dict[Iterable[Any], Any] = None,
        nrows: Union[int, None] = None,
    ) -> 'Generator':
        """
        Args:
            afile (str): File with contents to load.

            conso (Dict[Iterable[Any], Any]): Mapping with concepts as first
                element of keys.

            nrows (Union[int, None]): Maximum number of records to load from
                file. Default is None (all records).
        """
        valids = {'sty': ACCEPTED_SEMTYPES}
        if conso is not None:
            # NOTE: This version only considers CUIs that are found in the
            # concepts ('conso') data structure. It requires that the 'conso'
            # variable is in dictionary form. Although this approach increases
            # the installation time, it reduces the database size.
            valids['cui'] = {k[1] for k in conso.keys()}
        # return data_to_dict(
        return iter_data(
            afile,
            ['cui'],  # key
            ['sty'],  # values
            headers=HEADERS_MRSTY,
            valids=valids,
            nrows=nrows,
        )

    def _dump_conso(
        self,
        data: Iterable[Any],
        *,
        bulk_size: int = 1000,
        status_step: int = 10000,
    ) -> NoReturn:
        """Stores Term-CUI,Preferred mapping, term: [(CUI,pref), ...].

        Args:
            data (Iterable[Any]): Data to store.

            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()
        batch_per_step = bulk_size * (status_step / bulk_size)

        for i, ((term, cui), preferred) in enumerate(data, start=1):
            self._ss.insert(term)
            self._conso_db.set(
                term, (cui, preferred), replace=False, unique=True
            )

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s, '
                      f'{elapsed_time / batch_per_step} s/batch')
                prev_time = curr_time

        if VERBOSE:
            print(f'Num terms: {i}')

    def _dump_cuisty(
        self,
        data: Iterable[Any],
        *,
        bulk_size: int = 1000,
        status_step: int = 10000,
    ) -> NoReturn:
        """Stores CUI-Semantic Type mapping, cui: [sty, ...].

        Args:
            data (Iterable[Any]): Data to store.

            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()
        batch_per_step = bulk_size * (status_step / bulk_size)

        for i, (cui, sty) in enumerate(data, start=1):
            self._cuisty_db.set(cui, sty, replace=False, unique=True)

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s, '
                      f'{elapsed_time / batch_per_step} s/batch')
                prev_time = curr_time

        if VERBOSE:
            print(f'Num CUIs: {i}')

    def install(
        self,
        umls_dir: str,
        *,
        mrconso: str = 'MRCONSO.RRF',
        mrsty: str = 'MRSTY.RRF',
        **kwargs
    ) -> NoReturn:
        """
        Args:
            umls_dir (str): Directory of UMLS RRF files.

            mrconso (str): UMLS concepts file.
                Default is MRCONSO.RRF.

            mrsty (str): UMLS semantic types file.
                Default is MRSTY.RRF.

        Kwargs:
            Options passed directly to '_load_conso, _load_cuisty, _dump_conso,
            _dump_cuisty'.
        """
        nrows = kwargs.pop('nrows', None)

        t1 = time.time()

        print('Loading/parsing concepts...')
        start = time.time()
        mrconso_file = os.path.join(umls_dir, mrconso)
        conso = self._load_conso(mrconso_file, nrows=nrows)
        curr_time = time.time()
        print(f'Loading/parsing concepts: {curr_time - start} s')

        print('Writing concepts...')
        start = time.time()
        self._dump_conso(conso, **kwargs)
        curr_time = time.time()
        print(f'Writing concepts: {curr_time - start} s')

        print('Loading/parsing semantic types...')
        start = time.time()
        mrsty_file = os.path.join(umls_dir, mrsty)
        cuisty = self._load_sty(mrsty_file, nrows=nrows)
        curr_time = time.time()
        print(f'Loading/parsing semantic types: {curr_time - start} s')

        print('Writing semantic types...')
        start = time.time()
        self._dump_cuisty(cuisty, **kwargs)
        curr_time = time.time()
        print(f'Writing semantic types: {curr_time - start} s')

        t2 = time.time()
        print(f'Total runtime: {t2 - t1} s')


class Matcher:
    """FACET installation tool.

    Args:
        conso_db (BaseDatabase): Handle to database instance for CONCEPT-CUI
            storage.

        cuisty_db (BaseDatabase): Handle to database instance for CUI-STY
            storage.

        simstring (Simstring): Handle to Simstring instance.
            Simstring requires an internal database.

    Kwargs:
        language (str): spaCy language to use for processing corpora.
            Default is 'en'.

        stopwords (Set[str]): Domain-specific stopwords to complement spaCy's
            stopwords.
    """

    def __init__(
        self,
        *,
        conso_db: 'BaseDatabase' = DictDatabase(),
        cuisty_db: 'BaseDatabase' = DictDatabase(),
        simstring: 'Simstring' = Simstring(),
        **kwargs
    ):
        self._conso_db = conso_db
        self._cuisty_db = cuisty_db
        self._ss = simstring

        # Set up spaCy (model and stopwords)
        start = time.time()
        language = kwargs.pop('language', 'en')
        try:
            self._nlp = spacy.load(language)
        except KeyError:
            err = (f"Model for language '{language}' is not valid.")
            raise KeyError(err)
        except OSError:
            err = (f"Please run 'python3 -m spacy download {language}'")
            raise OSError(err)
        stopwords = kwargs.pop('stopwords', ())
        self._nlp.Defaults.stop_words.update(stopwords)
        curr_time = time.time()
        print(f'Loading spaCy model: {curr_time - start} s')

    def _make_token_sequences(
        self,
        sent: Iterable[str],
        *,
        window: int = 5,
        min_match_length: int = 3,
    ) -> Generator[Tuple[int, int, str], None, None]:
        """
        Args:
            sent (Iterable[str]): Text chunk to process.

            window (int): Window size for processing words.
                Default value is 5.

            min_match_length (int): Minimum number of characters for matching
                criterion. Default value is 3.

        Returns (Generator[Tuple[int, int, str]]: Span start, span end,
            and text.
        """
        for i in range(len(sent)):
            for j in range(i + 1, min(i + window, len(sent)) + 1):
                span = sent[i:j]
                if span.end_char - span.start_char >= min_match_length:
                    yield (span.start_char, span.end_char, span.text)

    def _make_ngrams(
        self,
        sent: Iterable[str],
        *,
        window: int = 5,
        min_match_length: int = 3,
    ) -> Generator[Tuple[int, int, str], None, None]:
        """
        Args:
            sent (Iterable[str]): Text chunk to process.

            window (int): Window size for processing words.
                Default value is 5.

            min_match_length (int): Minimum number of characters for matching
                criterion. Default value is 3.

        Returns (Generator[Tuple[int, int, str]]: Span start, span end,
            and text.
        """
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

        def _is_valid_token(token):
            return not(
                token.is_punct
                or token.is_space
                or token.pos_ in ('ADP', 'DET', 'CONJ')
            )

        def _is_valid_begin_token(token):
            return not(
                token.like_num
                or (token.text in self._nlp.Defaults.stop_words
                    and token.lemma_ not in NEGATIONS)
                or token.pos_ in ('ADP', 'DET', 'CONJ')
            )

        def _is_valid_middle_token(token):
            return (
                not(token.is_punct or token.is_space)
                or token.is_bracket
                or token.text in UNICODE_DASHES
            )

        def _is_valid_end_token(token):
            return not(
                token.is_punct
                or token.is_space
                or token.text in self._nlp.Defaults.stop_words
                or token.pos_ in ('ADP', 'DET', 'CONJ')
            )

        for i, token in enumerate(sent):
            if not _is_valid_token(token):
                continue

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == len(sent)
                and len(token) >= min_match_length
                and _is_valid_end_token(token)
            ):
                yield (token.idx, token.idx + len(token), token.text)
            else:
                span_start = i + 1 + int(not _is_valid_begin_token(token))
                span_end = min(len(sent), i + window) + 1
                for j in range(span_start, span_end):
                    if not _is_valid_middle_token(sent[j - 1]):
                        break

                    if not _is_valid_end_token(sent[j - 1]):
                        continue

                    span = sent[i:j]

                    if span.end_char - span.start_char < min_match_length:
                        continue

                    yield (
                        span.start_char, span.end_char,
                        ''.join(
                            token.text_with_ws
                            for token in span
                            if token.pos_ != 'DET'
                        ).strip()
                    )

    def _get_all_matches(
        self,
        ngrams: Iterable[Any],
        *,
        alpha: float = 0.7,
        # normalize_unicode: bool = True,
        # lowercase: bool = False,
    ) -> List[List[Dict[str, Any]]]:
        """
        Args:
            ngrams (Iterable[int, int, str]): Parsed N-grams with span.

            normalize_unicode (bool): Enable Unicode normalization.
                Default is True.
        """
        matches = []
        for begin, end, ngram in ngrams:
            # NOTE: Normalization should be done by the tokenizer not here.
            # ngram_normalized = ngram
            # if normalize_unicode:
            #     ngram_normalized = unidecode(ngram_normalized)

            # NOTE: Case matching is controlled by Simstring.
            # if lowercase:
            #     ngram_normalized = ngram_normalized.lower()

            # NOTE: Simstring results do not need to be sorted because
            # we sort matches based on similarity and preference.
            ngram_matches = []
            for candidate, similarity in self._ss.search(ngram,
                                                         alpha=alpha,
                                                         rank=False):
                for cui, pref in filter(None, self._conso_db.get(candidate)):
                    # NOTE: Using ACCEPTED_SEMTYPES will always result
                    # in true. If not so, should we include the match
                    # with a semtype=None or skip it?
                    semtypes = self._cuisty_db.get(cui)
                    if semtypes is not None:
                        ngram_matches.append({
                            'begin': begin,
                            'end': end,
                            'ngram': ngram,
                            'term': candidate,
                            'similarity': similarity,
                            'cui': cui,
                            'semantic_types': semtypes,
                            'preferred': pref,
                        })

            # Sort matches by similarity and preference
            if len(ngram_matches) > 0:
                ngram_matches.sort(
                    key=lambda x: x['similarity'] + int(x['preferred']),
                    reverse=True
                )
                matches.append(ngram_matches)
        return matches

    def _formatter(
        self,
        matches: Dict[str, List[Any]],
        *,
        formatter: Union[str, None] = None,
        outfile: str = None,
    ) -> str:
        """
        Args:
            matches (Dict[str, List[Any]]): Mapping of matches and attributes.

            formatter (Union[str, None]): Output format.
                Valid values are 'json', 'xml', 'pickle', 'csv', and None.
                Default is None.

            outfile (str): Output file. Default is to print output.
        """
        if formatter is None:
            formatted_matches = matches
        elif formatter.lower() == 'json':
            formatted_matches = json.JSONEncoder(indent=2).encode(matches)
        elif formatter.lower() == 'xml':
            formatted_matches = xml.dom.minidom.parseString(
                dicttoxml.dicttoxml(matches, attr_type=False)
            ).toprettyxml(indent=2 * ' ')
        elif formatter.lower() == 'pickle':
            formatted_matches = pickle.dumps(matches)
        elif formatter.lower() == 'csv':
            formatted_matches = matches
            if outfile is None:
                print('Warn: csv formatting only supported '
                      'for writing to file')
        else:
            raise ValueError(
                f'Error: invalid formatting, {formatter} is not supported'
            )

        if outfile is not None:
            if formatter.lower() == 'csv':
                with open(outfile, 'w', newline='') as fd:
                    keys = [
                        'begin', 'end', 'ngram', 'term', 'cui',
                        'similarity', 'semantic_types', 'preferred'
                    ]
                    writer = csv.DictWriter(fd, fieldnames=keys)
                    writer.writeheader()
                    # NOTE: Only works if query is raw text.
                    for text in matches['_text']:
                        for match in text:
                            writer.writerow(match)
            else:
                with open(outfile, 'w') as fd:
                    fd.write(formatted_matches)
        else:
            return formatted_matches

    def match(
        self,
        corpora: Union[str, Iterable[str]],
        *,
        best_match: bool = True,
        ignore_syntax: bool = False,
        alpha: float = 0.7,
        **kwargs
    ) -> Dict[str, List[List[Dict[str, Any]]]]:
        """
        Args:
            corpora (Union[str, Iterable[str]]): Corpora items.

            best_match (bool):

            ignore_syntax (bool): Ignore token types when parsing text.
                Default is False.

        Kwargs:
            Options passed directly to `corpus_generator`.

        Examples:

        >>> matches = FACET().match(['file1.txt', 'file2.txt', ...])
        >>> for terms in matches['file1.txt']:
        >>>     for term in terms:
        >>>         print(term['concept'], term['cui'], term['semantic_types'])
        """
        # Profile
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        matches = collections.defaultdict(list)
        for source, corpus in corpus_generator(corpora, **kwargs):
            # Creates a spaCy Doc object
            # len(Doc) == number of words
            start = time.time()
            doc = self._nlp.make_doc(corpus)
            curr_time = time.time()
            print(f'Num words: {len(doc)}')
            print(f'spaCy parse: {curr_time - start} s')

            start = curr_time
            if ignore_syntax:
                ngrams = self._make_token_sequences(doc)
            else:
                ngrams = self._make_ngrams(doc)
            curr_time = time.time()
            print(f'Make N-grams: {curr_time - start} s')

            start = curr_time
            _matches = self._get_all_matches(ngrams, alpha=alpha)
            curr_time = time.time()
            print(f'Num matches: {len(_matches)}')
            print(f'Get all matches: {curr_time - start} s')

            # if best_match:
            #     start = curr_time
            #     matches = self._select_terms(_matches)
            #     curr_time = time.time()
            #     print(f'Num best matches: {len(_matches)}')
            #     print(f'Select terms: {curr_time - start} s')

            # NOTE: Matches are not checked for duplication if placed
            # in the same key.
            if len(_matches) > 0:
                matches[source].extend(_matches)

        # Profile
        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return self._formatter(matches, **kwargs)
