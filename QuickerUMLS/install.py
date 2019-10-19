import os
import time
import collections
from unidecode import unidecode
from .database import DictDatabase
from .helpers import (
    load_data,
    corpus_generator,
)
from .umls_constants import (
    HEADERS_MRSTY,
    HEADERS_MRCONSO,
    ACCEPTED_SEMTYPES,
)
from typing import Any, Dict, Callable, Iterable


__all__ = ['Installer']


VERBOSE = True

# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


# TODO: Convert this class into an abstract class.
class Installer:
    """FACET installation tool.

    Args:
        conso_db (BaseDatabase): Handle to database instance for CONCEPT-CUI
            storage.

        cuisty_db (BaseDatabase): Handle to database instance for CUI-STY
            storage.

        simstring (Simstring): Handle to Simstring instance.
            The database handle should differ from the internal Simstring
            database handle (to prevent collisions with N-grams).

        bulk_size (int): Size of chunks to use for dumping data into
            databases. Default is 1000.

        status_step (int): Print status message after this number of
            records is dumped to databases. Default is 10000.
    """

    def __init__(
        self,
        *,
        conso_db: 'BaseDatabase',
        cuisty_db: 'BaseDatabase',
        simstring: 'Simstring',
        # TODO: Convert to the following arguments to **kwargs.
        bulk_size: int = 1000,
        status_step: int = 10000,
    ):
        self._conso_db = conso_db
        self._cuisty_db = cuisty_db
        self._ss = simstring
        self._bulk_size = bulk_size
        self._status_step = status_step

    def _load_conso(self, afile: str) -> Dict[Any, Any]:
        """
        Args:
            afile (str): File with contents to load.
        """
        def get_converters() -> Dict[str, Iterable[Callable]]:
            """Converter functions are used to process dataset during load
            operations.

            Converter functions should only take a single parameter
            (or use lambda and preset extra parameters).

            Returns (Dict[str, Iterable[Callable]]): Mapping of column IDs
                to list of functions.
            """
            converters = collections.defaultdict(list)
            converters['str'].append(unidecode)
            converters['str'].append(str.lower)
            return converters

        return load_data(
            afile,
            keys=['str'],
            values=['cui'],
            headers=HEADERS_MRCONSO,
            valids={'lat': ['ENG']},
            converters=get_converters(),
        )

    def _load_sty(self, afile: str) -> Dict[Any, Any]:
        """
        Args:
            afile (str): File with contents to load.
        """
        return load_data(
            afile,
            keys=['cui'],
            values=['sty'],
            headers=HEADERS_MRSTY,
            valids={'sty': ACCEPTED_SEMTYPES},
        )

    def _dump_conso(self, data: Any) -> Any:
        """Stores {Term:CUI} mapping, term: [CUI, ...].

        Args:
            data (Any): Data to store.
        """
        # Profile
        prev_time = time.time()
        batch_per_step = (self._bulk_size * (self._status_step /
                                             self._bulk_size))

        for i, (term, cui) in enumerate(data.items(), start=1):
            self._ss.insert(term)
            self._conso_db.set(term, cui)

            # Profile
            if VERBOSE and i % self._status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s, '
                      f'{elapsed_time / batch_per_step} s/batch')
                prev_time = curr_time

        if VERBOSE:
            print(f'Num terms: {i}')

    def _dump_cuisty(self, data: Any) -> Any:
        """Stores {CUI:Semantic Type} mapping, cui: [sty, ...].

        Args:
            data (Any): Data to store.
        """
        # Profile
        prev_time = time.time()
        batch_per_step = (self._bulk_size * (self._status_step /
                                             self._bulk_size))

        for i, (cui, sty) in enumerate(data.items(), start=1):
            self._cuisty_db.set(cui, sty)

            # Profile
            if VERBOSE and i % self._status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s, '
                      f'{elapsed_time / batch_per_step} s/batch')
                prev_time = curr_time

        if VERBOSE:
            print(f'Num CUIs: {i}')

    # NOTE: This method should only take **kwargs and this is a user-defined
    # dictionary passed to corresponding '_load_*()' and '_dump_*()' methods.
    def install(
        self,
        umls_dir: str,
        *,
        mrconso: str = 'MRCONSO.RRF',
        mrsty: str = 'MRSTY.RRF',
        **kwargs
    ):
        """
        Args:
            umls_dir (str): Directory of UMLS RRF files.

            mrconso (str): UMLS concepts file.
                Default is MRCONSO.RRF.

            mrsty (str): UMLS semantic types file.
                Default is MRSTY.RRF.

        Kwargs:
            Options passed directly to '_load_*()' and '_dump_*()' methods.
        """
        t1 = time.time()

        print('Loading/parsing concepts...')
        start = time.time()
        mrconso_file = os.path.join(umls_dir, mrconso)
        conso = self._load_conso(mrconso_file, **kwargs)
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
        cuisty = self._load_sty(mrsty_file, **kwargs)
        curr_time = time.time()
        print(f'Loading/parsing semantic types: {curr_time - start} s')

        print('Writing semantic types...')
        start = time.time()
        self._dump_cuisty(cuisty, **kwargs)
        curr_time = time.time()
        print(f'Writing semantic types: {curr_time - start} s')

        t2 = time.time()
        print(f'Total runtime: {t2 - t1} s')
