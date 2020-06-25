import time
from .utils import iload_data
from .base import BaseFacet
from unidecode import unidecode
from typing import (
    Any,
    List,
    Dict,
    Tuple,
)


__all__ = ['Facet']


VERBOSE = True

# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


class Facet(BaseFacet):
    """FACET text matcher."""

    def _install(self, data_file: str, *, overwrite: bool = True, **kwargs):
        """
        Args:
            data_file (str): File with data to install.

        Kwargs:
            Options passed directly to 'load_data()' function and '_dump_*()'
            methods.
        """
        t1 = time.time()

        print('Loading/parsing data...')
        start = time.time()
        data = iload_data(
            data_file,
            converters={'str': [unidecode, str.lower]},
            unique_keys=True,
            **kwargs,
        )
        curr_time = time.time()
        print(f'Loading/parsing data: {curr_time - start} s')

        print('Writing simstring...')
        start = time.time()
        # Stores Simstring inverted lists
        self._dump_simstring(data, **kwargs)
        curr_time = time.time()
        print(f'Writing simstring: {curr_time - start} s')

        t2 = time.time()
        print(f'Total runtime: {t2 - t1} s')

    def _match(
        self,
        ngram_struct: Tuple[int, int, str],
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        Args:
            ngram (Tuple[int, int, str]): Parsed N-grams with span.

        Kwargs:
            Options passed directly to `Simstring.search`.
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
            for candidate, similarity in self._simstring.search(
                ngram,
                **kwargs,
            )
        ]
