import os
import time
import itertools
import multiprocessing
from .helpers import load_data
from unidecode import unidecode
from .database import DictDatabase
from .umls_constants import (
    HEADERS_MRSTY,
    HEADERS_MRCONSO,
    ACCEPTED_SEMTYPES,
)


__all__ = ['ESInstaller']


VERBOSE = True

# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


# TODO: Convert this class into an abstract class.
class ESInstaller:
    """FACET installation tool.

    Args:
        cuisty_db (BaseDatabase): Handle to database instance for CUI-STY
            storage.

        simstring (Simstring): Handle to Simstring instance.
            The database handle should differ from the internal Simstring
            database handle (to prevent collisions with N-grams).
    """

    def __init__(self, *, cuisty_db: 'BaseDatabase', simstring: 'Simstring'):
        self._cuisty_db = cuisty_db
        self._ss = simstring

    def _dump_simstring(self, data, *, bulk_size=1000, status_step=10000):
        """Stores {Term:...} in Simstring database.

        Args:
            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()

        self._ss.db.set_pipe(True)
        for i, (term, cui) in enumerate(data.items(), start=1):
            self._ss.insert(term, cui)
            if i % bulk_size == 0:
                self._ss.db.sync()

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time
        self._ss.db.set_pipe(False)

        if VERBOSE:
            print(f'Num simstring terms: {i}')

    def _dump_cuisty(self, data, *, bulk_size=1000, status_step=10000):
        """Stores {CUI:Semantic Type} mapping, cui: [sty, ...].

        Args:
            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()

        self._cuisty_db.set_pipe(True)
        for i, (cui, sty) in enumerate(data.items(), start=1):
            self._cuisty_db.set(cui, sty)
            if i % bulk_size == 0:
                self._cuisty_db.sync()

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time
        self._cuisty_db.set_pipe(False)

        if VERBOSE:
            print(f'Num CUIs: {i}')

    # NOTE: This method should only take **kwargs and this is a user-defined
    # dictionary passed to corresponding '_load_*()' and '_dump_*()' methods.
    def install(
        self,
        umls_dir,
        *,
        mrconso='MRCONSO.RRF',
        mrsty='MRSTY.RRF',
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
        conso = load_data(
            data=mrconso_file,
            keys=['str'],
            values=['cui'],
            headers=HEADERS_MRCONSO,
            valids={'lat': ['ENG']},
            converters={'str': [unidecode, str.lower]},
            unique_values=True,
        )
        curr_time = time.time()
        print(f'Loading/parsing concepts: {curr_time - start} s')

        print('Loading/parsing semantic types...')
        start = time.time()
        mrsty_file = os.path.join(umls_dir, mrsty)
        cuisty = load_data(
            data=mrsty_file,
            keys=['cui'],
            values=['sty'],
            headers=HEADERS_MRSTY,
            valids={
                'sty': ACCEPTED_SEMTYPES,
                'cui': set(itertools.chain(*conso.values())),
            },
            unique_values=True,
        )
        curr_time = time.time()
        print(f'Loading/parsing semantic types: {curr_time - start} s')

        print('Writing simstring...')
        start = time.time()
        self._dump_simstring(conso, **kwargs)
        curr_time = time.time()
        print(f'Writing simstring: {curr_time - start} s')

        print('Writing semantic types...')
        start = time.time()
        self._dump_cuisty(cuisty, **kwargs)
        curr_time = time.time()
        print(f'Writing semantic types: {curr_time - start} s')

        t2 = time.time()
        print(f'Total runtime: {t2 - t1} s')
