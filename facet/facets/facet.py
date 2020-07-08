import time
from ..utils import iload_data
from .base import BaseFacet
from unidecode import unidecode
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Iterable,
)


__all__ = ['Facet']


VERBOSE = True

# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile  # noqa: F401


class Facet(BaseFacet):
    """FACET text matcher."""

    def _install(
        self,
        data: str,
        *,
        cols: Union[int, Iterable[int]] = 0,
        **kwargs,
    ):
        """Install.

        Args:
            data (str): File with data to install.

        Kwargs:
            Options passed directly to '*load_data()' function.
        """
        # Prepare 'keys' parameter as an iterable for 'load_data()'
        if isinstance(cols, (str, int)):
            cols = (cols,)

        t1 = time.time()

        print('Loading/parsing data...')
        start = time.time()
        data = iload_data(
            data,
            keys=cols,
            converters={cols[0]: [unidecode, str.lower]},
            **kwargs,
        )
        curr_time = time.time()
        print(f'Loading/parsing data: {curr_time - start} s')

        print('Writing matcher data...')
        start = time.time()
        # Stores Matcher-specific data
        self._dump_matcher(data)
        curr_time = time.time()
        print(f'Writing matcher data: {curr_time - start} s')

        t2 = time.time()
        print(f'Total runtime: {t2 - t1} s')

    def _match(
        self,
        ngram_struct: Tuple[int, int, str],
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Match.

        Args:
            ngram (Tuple[int, int, str]): Parsed N-grams with span.

        Kwargs:
            Options passed directly to `Matcher.search()`.
        """
        begin, end, ngram = ngram_struct
        return [
            {
                'begin': begin,
                'end': end,
                'ngram': ngram,
                'concept': candidate,
                'similarity': similarity,
            }
            for candidate, similarity in self._matcher.search(
                ngram,
                **kwargs,
            )
        ]
