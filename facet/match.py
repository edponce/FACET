import time
import collections
from .helpers import corpus_generator
from unidecode import unidecode
from .database import (
    database_map,
    BaseDatabase,
)
from .simstring import (
    simstring_map,
    BaseSimstring,
)
from .tokenizer import (
    tokenizer_map,
    BaseTokenizer,
)
from .formatter import Formatter
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
        conso_db (str, BaseDatabase): Handle to database instance or database
            name for CONCEPT-CUI storage. Valid database values are: 'dict',
            'redis', 'elasticsearch'. Default is 'dict'.

        cuisty_db (str, BaseDatabase): Handle to database instance or database
            name for CUI-STY storage. Valid database values are: 'dict',
            'redis', 'elasticsearch'. Default is 'dict'.

        simstring (str, BaseSimstring): Handle to Simstring instance or
            simstring name for inverted list of text. Valid simstring values
            are: 'simstring', 'elasticsearch'. Default is 'simstring'.

        tokenizer (str, BaseTokenizer): Tokenizer instance or tokenizer name.
            Valid tokenizers are: 'ws', 'nltk', 'spacy'. Default is 'ws'.

        formatter (Formatter): Formatter instance.
    """

    def __init__(
        self,
        *,
        conso_db: Union[str, 'BaseDatabase'] = 'dict',
        cuisty_db: Union[str, 'BaseDatabase'] = 'dict',
        simstring: Union[str, 'BaseSimstring'] = 'simstring',
        tokenizer: Union[str, 'BaseTokenizer'] = 'ws',
        formatter: 'Formatter' = Formatter(),
    ):
        self._conso_db = None
        self._cuisty_db = None
        self._ss = None
        self._tokenizer = None
        self.formatter = formatter

        self.conso_db = conso_db
        self.cuisty_db = cuisty_db
        self.tokenizer = tokenizer
        self.ss = simstring

    @property
    def conso_db(self):
        return self._conso_db

    @conso_db.setter
    def conso_db(self, value: Union[str, 'BaseDatabase']):
        obj = None
        if isinstance(value, str):
            obj = database_map[value]()
        elif isinstance(value, BaseDatabase):
            obj = value

        if obj is None:
            raise ValueError(f'invalid CONSO-CUI database, {value}')
        self._conso_db = obj

    @property
    def cuisty_db(self):
        return self._cuisty_db

    @cuisty_db.setter
    def cuisty_db(self, value: Union[str, 'BaseDatabase']):
        obj = None
        if isinstance(value, str):
            obj = database_map[value]()
        elif isinstance(value, BaseDatabase):
            obj = value

        if obj is None:
            raise ValueError(f'invalid CUI-STY database, {value}')
        self._cuisty_db = obj

    @property
    def ss(self):
        return self._ss

    @ss.setter
    def ss(self, value: Union[str, 'BaseSimstring']):
        obj = None
        if isinstance(value, str):
            obj = simstring_map[value]()
        elif isinstance(value, BaseSimstring):
            obj = value

        if obj is None:
            raise ValueError(f'invalid simstring, {value}')
        self._ss = obj

    @property
    def tokenizer(self):
        return self._tokenizer

    @tokenizer.setter
    def tokenizer(self, value: Union[str, 'BaseTokenizer']):
        obj = None
        if isinstance(value, str):
            obj = tokenizer_map[value]()
        elif isinstance(value, BaseTokenizer):
            obj = value

        if obj is None:
            raise ValueError(f'invalid tokenizer value, {value}')
        self._tokenizer = obj

    def _get_matches(
        self,
        ngrams: Iterable[Any],
        **kwargs,
    ) -> List[List[Dict[str, Any]]]:
        """
        Args:
            ngrams (Iterable[int, int, str]): Parsed N-grams with span.

        Kwargs:
            Options passed directly to `Simstring.search`.
        """
        matches = []
        for begin, end, ngram in ngrams:
            ngram_matches = []
            for candidate, similarity in self._ss.search(ngram, **kwargs):
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

            if len(ngram_matches) > 0:
                matches.append(ngram_matches)

        return matches

    def match(
        self,
        corpora: Union[str, Iterable[str]],
        *,
        # best_match: bool = True,
        case: str = None,
        normalize_unicode: bool = False,
        # NOTE: The following default values are based on the corresponding
        # function/method call using them.
        format: str = '',
        outfile: str = None,
        corpus_kwargs: Dict[str, Any] = {},
        **kwargs,
    ) -> Dict[str, List[List[Dict[str, Any]]]]:
        """
        Args:
            corpora (Union[str, Iterable[str]]): Corpora items.

            best_match (bool): ?

            case (str, None): Controls string casing during insert/search.
                Valid values are 'lL' (lower), 'uU' (upper), or None.
                Default is None.

            normalize_unicode (bool): Enable Unicode normalization.
                Default is False.

            format (str): Formatting mode for match results. Valid values
                are: 'json', 'xml', 'pickle', 'csv'.

            outfile (str): Output file for match results.

            corpus_kwargs (Dict[str, Any]): Options passed directly to
                `corpus_generator`.

        Kwargs:
            Options passed directly to `Simstring.search` via `_get_matches`.

        Examples:

        >>> matches = Facet().match(['file1.txt', 'file2.txt', ...])
        >>> for terms in matches['file1.txt']:
        >>>     for term in terms:
        >>>         print(term['concept'], term['cui'], term['semantic type'])

        >>> matches = Facet().match([('filename1', 'text1'), (...), ...])
        >>> for terms in matches['filename1']:
        >>>     for term in terms:
        >>>         print(term['concept'], term['cui'], term['semantic type'])
        """
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        if case in ('L', 'l'):
            strcase = str.lower
        elif case in ('U', 'u'):
            strcase = str.upper
        elif case is None:
            strcase = None
        else:
            raise ValueError(f'invalid string case option, {case}')

        t1 = time.time()
        matches = collections.defaultdict(list)
        for source, corpus in corpus_generator(corpora, **corpus_kwargs):
            if strcase is not None:
                corpus = strcase(corpus)

            if normalize_unicode:
                corpus = unidecode(corpus)

            for sentence in self._tokenizer.sentencize(corpus):
                ngrams = self._tokenizer.tokenize(sentence)
                _matches = self._get_matches(ngrams, **kwargs)

                # if best_match:
                #     matches = self._select_terms(_matches)
                #     print(f'Num best matches: {len(_matches)}')

                # NOTE: Matches are not checked for duplication if placed
                # in the same key.
                matches[source].extend(_matches)
        t2 = time.time()
        print(f'Matching N-grams: {t2 - t1} s')

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return self.formatter(matches, format=format, outfile=outfile)
