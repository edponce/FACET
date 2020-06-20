import os
import time
import itertools
import multiprocessing
from .helpers import load_data
from unidecode import unidecode
from .database import (
    database_map,
    BaseDatabase,
)
from .simstring import (
    simstring_map,
    BaseSimstring,
)
from typing import (
    Any,
    Dict,
    Union,
)
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
        cuisty_db (str, BaseDatabase): Handle to database instance or database
            name for CUI-STY storage. Valid database values are: 'dict',
            'redis', 'elasticsearch'. Default is 'dict'.

        simstring (str, BaseSimstring): Handle to Simstring instance or
            simstring name for inverted list of text. Valid simstring values
            are: 'simstring', 'elasticsearch'. Default is 'elasticsearch'.
    """

    def __init__(
        self,
        *,
        cuisty_db: Union[str, 'BaseDatabase'] = 'dict',
        simstring: Union[str, 'BaseSimstring'] = 'elasticsearch',
    ):
        self._cuisty_db = None
        self._ss = None

        self.cuisty_db = cuisty_db
        self.ss = simstring

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

    def _dump_simstring(
        self,
        data: Dict[str, Any],
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

    def _dump_kv(
        self,
        data: Dict[str, Any],
        db: 'BaseDatabase',
        *,
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

        db.set_pipe(True)
        for i, (key, val) in enumerate(data.items(), start=1):
            db.set(key, val)
            if i % bulk_size == 0:
                db.sync()

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s')
                prev_time = curr_time
        db.close()

        if VERBOSE:
            print(f'Num keys: {i}')

    def install(self, umls_dir: str, **kwargs):
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
        mrsty_file = os.path.join(umls_dir, 'MRSTY.RRF')
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
        # Stores {CUI:Semantic Type} mapping, cui: [sty, ...]
        self._dump_kv(cuisty, self._cuisty_db, **kwargs)
        curr_time = time.time()
        print(f'Writing semantic types: {curr_time - start} s')

        t2 = time.time()
        print(f'Total runtime: {t2 - t1} s')
