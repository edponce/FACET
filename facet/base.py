import time
import collections
# NOTE: Add multiprocessing
# import multiprocessing
from .utils import corpus_generator
from unidecode import unidecode
from .simstring import (
    simstring_map,
    BaseSimstring,
)
from .tokenizer import (
    tokenizer_map,
    BaseTokenizer,
)
from .formatter import (
    formatter_map,
    BaseFormatter,
)
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Iterable,
    Callable,
)


__all__ = ['BaseFacet']


VERBOSE = True

# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


class BaseFacet(ABC):
    """Class supporting FACET installers and matchers.

    Args:
        simstring (str, BaseSimstring): Handle to Simstring instance or
            simstring name for inverted list of text. Valid simstring values
            are: 'simstring', 'elasticsearch'.

        tokenizer (str, BaseTokenizer): Tokenizer instance or tokenizer name.
            Valid tokenizers are: 'simple', 'ws', 'nltk', 'spacy'.

        formatter (str, BaseFormatter): Formatter instance or formatter name.
            Valid formatters are: 'json', 'yaml', 'xml', 'pickle', 'csv'.
    """

    def __init__(
        self,
        *,
        simstring: Union[str, 'BaseSimstring'] = 'simstring',
        tokenizer: Union[str, 'BaseTokenizer'] = 'ws',
        formatter: Union[str, 'BaseFormatter'] = None,
    ):
        self._simstring = None
        self._tokenizer = None
        self._formatter = None

        self._set_simstring(simstring)
        self.tokenizer = tokenizer
        self.formatter = formatter

    @property
    def simstring(self):
        return self._simstring

    def _set_simstring(self, value: Union[str, 'BaseSimstring']):
        obj = None
        if isinstance(value, str):
            obj = simstring_map[value]()
        elif isinstance(value, BaseSimstring):
            obj = value

        if obj is None:
            raise ValueError(f'invalid simstring, {value}')
        self._simstring = obj

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
        case: str = None,
        normalize_unicode: bool = False,
        # NOTE: The following default values are based on the corresponding
        # function/method call using them.
        format: str = '',
        outfile: str = None,
        corpus_kwargs: Dict[str, Any] = {},
        **kwargs,
    ) -> Dict[str, List[List[Dict[str, Any]]]]:
        """Match queries from corpora.

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
            Options passed directly to `Simstring.search` via `_match`.

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

        casefunc = self._get_case_func(case)

        t1 = time.time()
        matches = collections.defaultdict(list)
        for source, corpus in corpus_generator(corpora, **corpus_kwargs):
            corpus = casefunc(corpus)

            if normalize_unicode:
                corpus = unidecode(corpus)

            for sentence in self._tokenizer.sentencize(corpus):
                for ngram_struct in self._tokenizer.tokenize(sentence):
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

        formatter = (
            self._formatter
            if format == ''
            else formatter_map[format]()
        )
        return formatter(matches, outfile=outfile)

    def install(self, data, *, overwrite: bool = True, **kwargs):
        """Install data into Simstring database.

        Args:
            data_file (str): File with data to install.

            overwrite (bool): If set, overwrite previous data in Simstring
                database.

        Kwargs:
            Options passed directly to 'load_data()' function and '_dump_*()'
            method via `_install`.
        """
        # Clear databases
        if overwrite:
            if self._simstring is not None and self._simstring.db is not None:
                self._simstring.db.clear()
        self._install(data, overwrite=overwrite, **kwargs)

    def close(self):
        self._simstring.db.close()
        self._close()

    def _dump_simstring(
        self,
        data: Iterable[str],
        *,
        bulk_size: int = 1000,
        status_step: int = 10000,
    ):
        """Stores {Term:...} in Simstring database.

        Args:
            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()

        # NOTE: Pipeline mode does works for all databases because
        # 'simstring.insert' requires reading from database.
        # Enable pipeline mode
        # self._simstring.db.set_pipe(True)

        i = 0
        for term in data:
            i += 1
            self._simstring.insert(term)
            # if i % bulk_size == 0:
            #     self._simstring.db.sync()

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time

        # Disable pipeline mode
        # NOTE: Stores data remaining in pipeline.
        # self._simstring.db.set_pipe(False)

        if VERBOSE:
            print(f'Records processed: {i}')
            print(f'Simstring records: {len(self._simstring.db)}')

    def _dump_kv(
        self,
        data: Iterable[Tuple[str, Any]],
        *,
        db: 'BaseDatabase',
        bulk_size: int = 1000,
        status_step: int = 10000,
    ):
        """Stores {key:val} mapping, key: [val, ...].

        Args:
            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()

        # Enable pipeline mode
        db.set_pipe(True)

        i = 0
        for key, val in data:
            i += 1
            db.set(key, val)
            if i % bulk_size == 0:
                db.sync()

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time

        # Disable pipeline mode
        # NOTE: Stores data remaining in pipeline.
        self._simstring.db.set_pipe(False)
        db.set_pipe(False)

        if VERBOSE:
            print(f'Records processed: {i}')
            print(f'Key/value records: {len(db)}')

    def _dump_simstring_kv(
        self,
        data: Iterable[Tuple[str, Any]],
        *,
        db: 'BaseDatabase',
        bulk_size: int = 1000,
        status_step: int = 10000,
    ):
        """Stores {Term:...} in Simstring database and stores {key:val}
        mapping, key: [val, ...].

        Args:
            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()

        # Enable pipeline mode
        # NOTE: Pipeline mode does works for all databases because
        # 'simstring.insert' requires reading from database.
        # self._simstring.db.set_pipe(True)
        db.set_pipe(True)

        i = 0
        for key, val in data:
            i += 1
            self._simstring.insert(key)
            db.set(key, val)
            if i % bulk_size == 0:
                # self._simstring.db.sync()
                db.sync()

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time

        # Disable pipeline mode
        # NOTE: Stores data remaining in pipeline.
        # self._simstring.db.set_pipe(False)
        db.set_pipe(False)

        if VERBOSE:
            print(f'Records processed: {i}')
            print(f'Key/value records: {len(db)}')
            print(f'Simstring records: {len(self._simstring.db)}')

    def _get_case_func(self, case: str = None) -> Callable[[str], str]:
        if case in {'L', 'l'}:
            func = str.lower
        elif case in {'U', 'u'}:
            func = str.upper
        elif case is None:
            func = str
        else:
            raise ValueError(f'invalid string case option, {case}')
        return func

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
    def _install(self, data, *, overwrite: str = True, **kwargs):
        pass
