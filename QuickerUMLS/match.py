import time
import collections
from .formatter import Formatter
from .helpers import corpus_generator
from .tokenizer import NLTKTokenizer as Tokenizer
from typing import (
    Any,
    List,
    Dict,
    Union,
    Iterable,
)


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
    """

    def __init__(
        self,
        *,
        conso_db: 'BaseDatabase',
        cuisty_db: 'BaseDatabase',
        simstring: 'Simstring',
        tokenizer: 'BaseTokenizer' = Tokenizer(),
    ):
        self._conso_db = conso_db
        self._cuisty_db = cuisty_db
        self._ss = simstring
        self.tokenizer = tokenizer
        self.formatter = Formatter()

    # def __call__(self, corpora: Union[str, Iterable[str]], **kwargs):
    #     """
    #     Kwargs:
    #         Options passed directly to `match`.
    #     """
    #     return self.match(corpora, **kwargs)

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
            # we sort matches based on similarity
            ngram_matches = []
            for candidate, similarity in self._ss.search(ngram,
                                                         alpha=alpha,
                                                         rank=False):
                for cui in filter(None, self._conso_db.get(candidate)):
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
                        })

            # Sort matches by similarity
            # NOTE: If Simstring results are sorted the same way, then
            # we can skip this extra sorting.
            if len(ngram_matches) > 0:
                ngram_matches.sort(
                    key=lambda x: x['similarity'],
                    reverse=True
                )
                matches.append(ngram_matches)
        return matches

    def __call__(
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

        t1 = time.time()
        matches = collections.defaultdict(list)
        for source, corpus in corpus_generator(corpora, **kwargs):
            for sentence in self.tokenizer.sentencize(corpus):
                ngrams = self.tokenizer.tokenize(sentence)

                _matches = self._get_matches(ngrams, alpha=alpha)
                print(f'Num matches: {len(_matches)}')

                # if best_match:
                #     matches = self._select_terms(_matches)
                #     print(f'Num best matches: {len(_matches)}')

                # NOTE: Matches are not checked for duplication if placed
                # in the same key.
                matches[source].extend(_matches)
        t2 = time.time()
        print(f'Matching N-grams: {t2 - t1} s')

        # Profile
        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return self.formatter(matches)
