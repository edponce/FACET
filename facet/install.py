import os
import time
import itertools
from .helpers import load_data
from unidecode import unidecode
from .umls_constants import (
    HEADERS_MRSTY,
    HEADERS_MRCONSO,
    ACCEPTED_SEMTYPES,
)


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
    """

    def __init__(
        self,
        *,
        conso_db: 'BaseDatabase',
        cuisty_db: 'BaseDatabase',
        simstring: 'Simstring',
    ):
        self._conso_db = conso_db
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
        for i, term in enumerate(data.keys(), start=1):
            self._ss.insert(term)
            if i % bulk_size == 0:
                self._ss.db.sync()

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time
        self._ss.db.close()

        if VERBOSE:
            print(f'Num simstring terms: {i}')

    def _dump_kv(self, data, db, *, bulk_size=1000, status_step=10000):
        """Stores {key:val} mapping, key: [val, ...].

        Args:
            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()

        db.set_pipe(True)
        for i, (key, val) in enumerate(data.items(), start=1):
            self._conso_db.set(key, val)
            if i % bulk_size == 0:
                self._conso_db.sync()

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time
        db.close()

        if VERBOSE:
            print(f'Num keys: {i}')

    def install(
        self,
        umls_dir,
        **kwargs
    ):
        """
        Args:
            umls_dir (str): Directory of UMLS RRF files.

        Kwargs:
            Options passed directly to '_dump_*()' methods.
        """
        t1 = time.time()

        print('Loading/parsing concepts...')
        start = time.time()
        mrconso_file = os.path.join(umls_dir, 'MRCONSO.RRF')
        conso = load_data(
            mrconso_file,
            keys=['str'],
            values=['cui'],
            headers=HEADERS_MRCONSO,
            valids={'lat': ['ENG']},
            converters={'str': [unidecode, str.lower]},
            unique_values=True,
        )
        curr_time = time.time()
        print(f'Loading/parsing concepts: {curr_time - start} s')

        print('Writing concepts...')
        start = time.time()
        # Stores {Term:CUI} mapping, term: [CUI, ...]
        self._dump_kv(conso, self._conso_db, **kwargs)
        curr_time = time.time()
        print(f'Writing concepts: {curr_time - start} s')

        print('Writing simstring...')
        start = time.time()
        self._dump_simstring(conso, **kwargs)
        curr_time = time.time()
        print(f'Writing simstring: {curr_time - start} s')

        # NOTE: 'valids' option does not removes keys if no value is
        # available.
        print('Loading/parsing semantic types...')
        start = time.time()
        mrsty_file = os.path.join(umls_dir, 'MRSTY.RRF')
        cuisty = load_data(
            mrsty_file,
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

        print('Writing semantic types...')
        start = time.time()
        # Stores {CUI:Semantic Type} mapping, cui: [sty, ...]
        self._dump_kv(cuisty, self._cuisty_db, **kwargs)
        curr_time = time.time()
        print(f'Writing semantic types: {curr_time - start} s')

        t2 = time.time()
        print(f'Total runtime: {t2 - t1} s')
