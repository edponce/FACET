import time
import collections
from abc import (
    ABC,
    abstractmethod,
)
from unidecode import unidecode
from ..utils import corpus_generator
from ..matcher import (
    matcher_map,
    BaseMatcher,
)
from ..tokenizer import (
    tokenizer_map,
    BaseTokenizer,
)
from ..formatter import (
    formatter_map,
    BaseFormatter,
)
from ..database import (
    database_map,
    BaseDatabase,
)
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Iterable,
)


__all__ = ['BaseFacet']


VERBOSE = True

# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


strcase_map = {
    'l': str.lower,
    'L': str.lower,
    'u': str.upper,
    'U': str.upper,
    None: str,
}


def create_proxy_db():
    return database_map['dict']()


class BaseFacet(ABC):
    """Class supporting FACET installers and matchers.

    Args:
        matcher (str, BaseMatcher): Handle to Simstring instance or
            matcher name for inverted list of text. Valid matcher values
            are: 'simstring', 'elasticsearch'.

        tokenizer (str, BaseTokenizer): Tokenizer instance or tokenizer name.
            Valid tokenizers are: 'basic', 'ws', 'nltk', 'spacy'.

        formatter (str, BaseFormatter): Formatter instance or formatter name.
            Valid formatters are: 'json', 'yaml', 'xml', 'pickle', 'csv'.

        use_proxy_install (bool): If set, an in-memory database will be used
            for installation, then data will be dumped into selected databases.
    """

    def __init__(
        self,
        *,
        matcher: Union[str, 'BaseMatcher'] = 'simstring',
        tokenizer: Union[str, 'BaseTokenizer'] = 'ws',
        formatter: Union[str, 'BaseFormatter'] = None,
        use_proxy_install: bool = False,
    ):
        self._matcher = None
        self._tokenizer = None
        self._formatter = None
        self._use_proxy_install = use_proxy_install

        self.matcher = matcher
        self.tokenizer = tokenizer
        self.formatter = formatter

    @property
    def matcher(self):
        return self._matcher

    @matcher.setter
    def matcher(self, value: Union[str, 'BaseMatcher']):
        if isinstance(value, str):
            obj = matcher_map[value]()
        elif isinstance(value, BaseMatcher):
            obj = value
        else:
            raise ValueError(f'invalid matcher, {value}')
        self._matcher = obj

    @property
    def tokenizer(self):
        return self._tokenizer

    @tokenizer.setter
    def tokenizer(self, value: Union[str, 'BaseTokenizer']):
        if value is None or isinstance(value, str):
            obj = tokenizer_map[value]()
        elif isinstance(value, BaseTokenizer):
            obj = value
        else:
            raise ValueError(f'invalid tokenizer value, {value}')
        self._tokenizer = obj

    @property
    def formatter(self):
        return self._formatter

    @formatter.setter
    def formatter(self, value: Union[str, 'BaseFormatter']):
        if value is None or isinstance(value, str):
            obj = formatter_map[value]()
        elif isinstance(value, BaseFormatter):
            obj = value
        else:
            raise ValueError(f'invalid formatter value, {value}')
        self._formatter = obj

    def match(
        self,
        corpora: Union[str, Iterable[str]],
        *,
        # best_match: bool = True,
        case: str = 'l',
        normalize_unicode: bool = False,
        # NOTE: The following default values are based on the corresponding
        # function/method call using them.
        formatter: str = '',
        tokenizer: str = '',
        output: str = None,
        corpus_kwargs: Dict[str, Any] = {},
        **kwargs,
    ) -> Dict[str, List[List[Dict[str, Any]]]]:
        """Match queries from corpora.

        Args:
            corpora (Union[str, Iterable[str]]): Corpora items.

            best_match (bool): ?

            case (str, None): Controls string casing during insert/search.
                Valid values are 'lL' (lower), 'uU' (upper), or None.

            normalize_unicode (bool): Enable Unicode normalization.

            formatter (str): Formatter name. Valid formatters are: 'json',
                'yaml', 'xml', 'pickle', 'csv'.

            tokenizer (str): Tokenizer name. Valid tokenizers are: 'basic',
                'ws', 'nltk', 'spacy'.

            output (str): Output file for match results.

            corpus_kwargs (Dict[str, Any]): Options passed directly to
                `corpus_generator`.

        Kwargs:
            Options passed directly to `Matcher.search` via `_match`.

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
        # Resolve formatter
        formatter = (
            self._formatter
            if formatter == ''
            else formatter_map[formatter]()
        )

        # Resolve tokenizer
        tokenizer = (
            self._tokenizer
            if tokenizer == ''
            else tokenizer_map[tokenizer]()
        )

        casefunc = strcase_map[case]

        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        t1 = time.time()
        matches = collections.defaultdict(list)
        for source, corpus in corpus_generator(corpora, **corpus_kwargs):
            corpus = casefunc(corpus)

            if normalize_unicode:
                corpus = unidecode(corpus)

            for sentence in tokenizer.sentencize(corpus):
                for ngram_struct in tokenizer.tokenize(sentence):
                    ngram_matches = self._match(ngram_struct, **kwargs)
                    if len(ngram_matches) == 0:
                        continue

                    # if best_match:
                    #     ngram_matches = self._select_terms(ngram_matches)
                    #     print(f'Num best matches: {len(ngram_matches)}')

                    # NOTE: Matches are not checked for duplication if placed
                    # in the same key.
                    matches[source].append(ngram_matches)
        t2 = time.time()
        print(f'Matching N-grams: {t2 - t1} s')

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

        return formatter(matches, output=output)

    def install(self, data, **kwargs):
        """Install data.

        Args:
            data (str): File with data to install.

        Kwargs:
            Options passed directly to '*load_data()' function method via
            `_install()`.
        """
        if PROFILE:
            prof = cProfile.Profile(subcalls=True, builtins=True)
            prof.enable()

        self._install(data, **kwargs)

        if PROFILE:
            prof.disable()
            prof.create_stats()
            prof.print_stats('time')
            prof.clear()

    def close(self):
        self._matcher.db.close()
        self._close()

    def _dump_matcher(
        self,
        data: Iterable[str],
        *,
        bulk_size: int = 10000,
        status_step: int = 10000,
    ):
        """Stores {Term:...} in Matcher database.

        Args:
            bulk_size (int): Size of chunks to use for dumping data into
                databases.

            status_step (int): Print status message after this number of
                records is dumped to databases.
        """
        # Set up proxy database
        if self._use_proxy_install:
            orig_db = self._matcher.db
            proxy_db = create_proxy_db()
            self._matcher.db = proxy_db

        prev_time = time.time()

        i = 0
        for term in data:
            i += 1
            self._matcher.insert(term)
            if i % bulk_size == 0:
                self._matcher.db.commit()

            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time

        self._matcher.db.commit()

        if VERBOSE:
            print(f'Records processed: {i}')
            print(f'Matcher records: {len(self._matcher.db)}')

        # Copy proxy database
        if self._use_proxy_install:
            proxy_db.copy(orig_db)
            self._matcher.db = orig_db
            proxy_db.clear()

    def _dump_kv(
        self,
        data: Iterable[Tuple[str, Any]],
        *,
        db: 'BaseDatabase',
        bulk_size: int = 10000,
        status_step: int = 10000,
    ):
        """Stores {key:val} mapping, key: [val, ...].

        Args:
            bulk_size (int): Size of chunks to use for dumping data into
                databases.

            status_step (int): Print status message after this number of
                records is dumped to databases.
        """
        # Set up proxy database
        if self._use_proxy_install:
            orig_db = db
            proxy_db = create_proxy_db()
            db = proxy_db

        prev_time = time.time()

        i = 0
        for key, val in data:
            i += 1
            db.set(key, val)
            if i % bulk_size == 0:
                db.commit()

            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time

        db.commit()

        if VERBOSE:
            print(f'Records processed: {i}')
            print(f'Key/value records: {len(db)}')

        # Copy proxy database
        if self._use_proxy_install:
            proxy_db.copy(orig_db)
            db = orig_db
            proxy_db.clear()

    def _dump_matcher_kv(
        self,
        data: Iterable[Tuple[str, Any]],
        *,
        db: 'BaseDatabase',
        bulk_size: int = 10000,
        status_step: int = 10000,
    ):
        """Stores {Term:...} in Matcher database and stores {key:val}
        mapping, key: [val, ...].

        Args:
            bulk_size (int): Size of chunks to use for dumping data into
                databases.

            status_step (int): Print status message after this number of
                records is dumped to databases.
        """
        # Set up proxy database
        if self._use_proxy_install:
            orig_db = self._matcher.db
            proxy_db = create_proxy_db()
            self._matcher.db = proxy_db

            orig_db2 = db
            proxy_db2 = create_proxy_db()
            db = proxy_db2

        prev_time = time.time()

        i = 0
        for key, val in data:
            i += 1
            self._matcher.insert(key)
            db.set(key, val)
            if i % bulk_size == 0:
                self._matcher.db.commit()
                db.commit()

            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time

        self._matcher.db.commit()
        db.commit()

        if VERBOSE:
            print(f'Records processed: {i}')
            print(f'Key/value records: {len(db)}')
            print(f'Matcher records: {len(self._matcher.db)}')

        # Copy proxy database
        if self._use_proxy_install:
            proxy_db.copy(orig_db)
            self._matcher.db = orig_db
            proxy_db.clear()

            proxy_db2.copy(orig_db2)
            db = orig_db2
            proxy_db2.clear()

    def _close(self):
        pass

    @abstractmethod
    def _match(
        self,
        ngram_struct: Tuple[int, int, str],
        **kwargs,
    ) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def _install(self, data, **kwargs):
        pass
