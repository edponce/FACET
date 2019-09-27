import csv
import time
import json
import pickle
import dicttoxml
import collections
import xml.dom.minidom
from QuickerUMLS.simstring import Simstring
from QuickerUMLS.database import DictDatabase
from QuickerUMLS.tokenizer import SpacyTokenizer
from QuickerUMLS.helpers import corpus_generator
from typing import Any, Union, List, Dict, Iterable


__all__ = ['Facet']


# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


class Facet:
    """FACET installation tool.

    Args:
        conso_db (BaseDatabase): Handle to database instance for CONCEPT-CUI
            storage.

        cuisty_db (BaseDatabase): Handle to database instance for CUI-STY
            storage.

        simstring (Simstring): Handle to Simstring instance.
            Simstring requires an internal database.

        tokenizer (BaseTokenizer): Tokenizer instance.
            Default is SpacyTokenizer.
    """

    # NOTE: Need to remove default parameters because non-empty databases
    # are required, but for debugging empty databases are useful.
    def __init__(
        self,
        *,
        conso_db: 'BaseDatabase' = DictDatabase(),
        cuisty_db: 'BaseDatabase' = DictDatabase(),
        simstring: 'Simstring' = Simstring(),
        tokenizer: 'BaseTokenizer' = SpacyTokenizer(),
    ):
        self._conso_db = conso_db
        self._cuisty_db = cuisty_db
        self._ss = simstring
        self._tokenizer = tokenizer

    def __call__(self, corpora: Union[str, Iterable[str]]):
        return self.match(corpora)

    def _get_matches(
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
                            'concept': candidate,
                            'similarity': similarity,
                            'cui': cui,
                            'semantic type': semtypes,
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
        formatter: str = None,
        outfile: str = None,
    ) -> str:
        """
        Args:
            matches (Dict[str, List[Any]]): Mapping of matches and attributes.

            formatter (str): Output format.
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
                        'begin', 'end', 'ngram', 'concept', 'cui',
                        'similarity', 'semantic type', 'preferred'
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
        alpha: float = 0.7,
        **kwargs
    ) -> Dict[str, List[List[Dict[str, Any]]]]:
        """
        Args:
            corpora (Union[str, Iterable[str]]): Corpora items.

            best_match (bool):

        Kwargs:
            Options passed directly to `corpus_generator`.

        Examples:

        >>> matches = Facet().match(['file1.txt', 'file2.txt', ...])
        >>> for terms in matches['file1.txt']:
        >>>     for term in terms:
        >>>         print(term['concept'], term['cui'], term['semantic type'])
        """
        # Profile
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        matches = collections.defaultdict(list)
        for source, corpus in corpus_generator(corpora, **kwargs):
            start = time.time()
            for sentence in self._tokenizer.sentencize(corpus):
                ngrams = self._tokenizer.tokenize(sentence)
                curr_time = time.time()
                print(f'Make N-grams: {curr_time - start} s')

                start = curr_time
                _matches = self._get_matches(ngrams, alpha=alpha)
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
                matches[source].extend(_matches)

        # Profile
        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return self._formatter(matches, **kwargs)
