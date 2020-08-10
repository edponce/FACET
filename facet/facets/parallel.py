import time
import functools
import collections
import multiprocessing
# NOTE: How to include facetFactory?
import facet
# from ..factory import FacetFactory
from ..formatter import get_formatter
from ..helpers import corpus_generator
from typing import (
    Any,
    Dict,
    Union,
    Iterable,
)


__all__ = ['ParallelFacet']


# NOTE: This class is not subclassed because it is agnostic of FACET
# classes. It requires a configuration to be instantiated and only supports
# the match() operation.
# NOTE: This class can have the same API as BaseFacet, maybe even subclass it.
class ParallelFacet:
    """Multiprocessing FACET for matching only."""

    NAME = 'parallel'

    def __init__(
        self,
        config: Union[str, Dict[str, Any]],
        num_procs: int = multiprocessing.cpu_count(),
    ):
        self.num_procs = num_procs
        self._factory = facet.FacetFactory(config)
        self._formatter = get_formatter(
            self._factory.get_config().pop('formatter', None)
        )

    @staticmethod
    def _worker(corpora, *, factory, kwargs):
        f = factory.create()
        matches = f.match(corpora, **kwargs)
        f.close()
        return matches

    def match(
        self,
        corpora: Union[str, Iterable[str]],
        bulk_size: int = 1,
        **kwargs,
    ):
        """
        Notes:
            * To increase performance for large data sets, increase the
              bulk_size parameter. This allows reusing more effectively
              the cache database (if enabled). Ideally use a cache database
              that supports concurrent writes and persists.

        Kwargs:
            Options forwarded to 'match()' method of FACET workers.
        """
        # Disable formatter for workers, this object will manage formatting.
        formatter = get_formatter(kwargs.pop('formatter', self._formatter))
        kwargs['formatter'] = None
        output = kwargs.pop('output', None)
        kwargs['output'] = None

        # Set corpus extraction for sources only
        corpus_kwargs = kwargs.pop('corpus_kwargs', {})
        corpus_kwargs['source_only'] = True

        func = functools.partial(
            self._worker,
            factory=self._factory,
            kwargs=kwargs,
        )

        t1 = time.time()

        # NOTE: Need to compare with map_async()
        # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool.map_async
        all_matches = collections.defaultdict(list)
        with multiprocessing.Pool(processes=self.num_procs) as pool:
            # Combine matches from workers
            for matches in pool.imap_unordered(
                func,
                # Extract corpus items to distribute among workers
                corpus_generator(corpora, **corpus_kwargs),
                chunksize=bulk_size,
            ):
                for k, v in matches.items():
                    all_matches[k].extend(v)

        t2 = time.time()
        print(f'Matching all N-grams: {t2 - t1} s')

        return formatter(all_matches, output=output)

    def close(self):
        pass
