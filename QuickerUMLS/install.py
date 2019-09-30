import os
import time
import collections
from unidecode import unidecode
from QuickerUMLS.helpers import (
    # data_to_dict,
    iter_data,
    is_iterable,
    corpus_generator,
)
from QuickerUMLS.umls_constants import (
    HEADERS_MRSTY,
    HEADERS_MRCONSO,
    ACCEPTED_SEMTYPES,
)
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Callable,
    Iterable,
    NoReturn,
    Generator,
)


__all__ = ['Installer']


VERBOSE = True

# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


class Installer:
    """FACET installation tool.

    Kwargs:
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

    def _load_conso(
        self,
        afile: str,
        *,
        language: Union[str, Iterable[str]] = ['ENG'],
        nrows: Union[int, None] = None,
    ) -> 'Generator':
        """
        Args:
            afile (str): File with contents to load.

            language (Union[str, Iterable[str]]): Extract concepts of the
                specified languages. Default is 'ENG'.

            nrows (Union[int, None]): Maximum number of records to load
                from file. Default is None (all records).
        """
        def configure_converters() -> Dict[str, List[Callable]]:
            """Converter functions are used to process dataset during load
            operations.

            Converter functions should only take a single parameter
            (or use lambda and preset extra parameters).

            Returns (Dict[str, List[Callable]]): Mapping of column IDs
                to list of functions.
            """
            converters = collections.defaultdict(list)
            converters['str'].append(str.lower)
            converters['str'].append(unidecode)
            converters['ispref'].append(lambda v: True if v == 'Y' else False)
            return converters

        if not is_iterable(language):
            language = [language]

        # NOTE: This version returns a dictionary data structure
        # which is necessary if semantic types are going to be used as a
        # filter for CUIs. See NOTE in loading of semantic types.
        # return data_to_dict(
        return iter_data(
            afile,
            ['str', 'cui'],  # key
            ['ispref'],      # values
            headers=HEADERS_MRCONSO,
            valids={'lat': language},
            converters=configure_converters(),
            unique_keys=True,
            nrows=nrows,
        )

    def _load_sty(
        self,
        afile: str,
        *,
        conso: Dict[Iterable[Any], Any] = None,
        nrows: Union[int, None] = None,
    ) -> 'Generator':
        """
        Args:
            afile (str): File with contents to load.

            conso (Dict[Iterable[Any], Any]): Mapping with concepts as first
                element of keys.

            nrows (Union[int, None]): Maximum number of records to load from
                file. Default is None (all records).
        """
        valids = {'sty': ACCEPTED_SEMTYPES}
        if conso is not None:
            # NOTE: This version only considers CUIs that are found in the
            # concepts ('conso') data structure. It requires that the 'conso'
            # variable is in dictionary form. Although this approach increases
            # the installation time, it reduces the database size.
            valids['cui'] = {k[1] for k in conso.keys()}
        # return data_to_dict(
        return iter_data(
            afile,
            ['cui'],  # key
            ['sty'],  # values
            headers=HEADERS_MRSTY,
            valids=valids,
            nrows=nrows,
        )

    def _dump_conso(
        self,
        data: Iterable[Any],
        *,
        bulk_size: int = 1000,
        status_step: int = 10000,
    ) -> NoReturn:
        """Stores Term-CUI,Preferred mapping, term: [(CUI,pref), ...].

        Args:
            data (Iterable[Any]): Data to store.

            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()
        batch_per_step = bulk_size * (status_step / bulk_size)

        for i, ((term, cui), preferred) in enumerate(data, start=1):
            self._ss.insert(term)
            self._conso_db.set(
                term, (cui, preferred), replace=False, unique=True
            )

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s, '
                      f'{elapsed_time / batch_per_step} s/batch')
                prev_time = curr_time

        if VERBOSE:
            print(f'Num terms: {i}')

    def _dump_cuisty(
        self,
        data: Iterable[Any],
        *,
        bulk_size: int = 1000,
        status_step: int = 10000,
    ) -> NoReturn:
        """Stores CUI-Semantic Type mapping, cui: [sty, ...].

        Args:
            data (Iterable[Any]): Data to store.

            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
        """
        # Profile
        prev_time = time.time()
        batch_per_step = bulk_size * (status_step / bulk_size)

        for i, (cui, sty) in enumerate(data, start=1):
            self._cuisty_db.set(cui, sty, replace=False, unique=True)

            # Profile
            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                elapsed_time = curr_time - prev_time
                print(f'{i}: {elapsed_time} s, '
                      f'{elapsed_time / batch_per_step} s/batch')
                prev_time = curr_time

        if VERBOSE:
            print(f'Num CUIs: {i}')

    def install(
        self,
        umls_dir: str,
        *,
        mrconso: str = 'MRCONSO.RRF',
        mrsty: str = 'MRSTY.RRF',
        **kwargs
    ) -> NoReturn:
        """
        Args:
            umls_dir (str): Directory of UMLS RRF files.

            mrconso (str): UMLS concepts file.
                Default is MRCONSO.RRF.

            mrsty (str): UMLS semantic types file.
                Default is MRSTY.RRF.

        Kwargs:
            Options passed directly to '_load_conso, _load_cuisty, _dump_conso,
            _dump_cuisty'.
        """
        nrows = kwargs.pop('nrows', None)

        t1 = time.time()

        print('Loading/parsing concepts...')
        start = time.time()
        mrconso_file = os.path.join(umls_dir, mrconso)
        conso = self._load_conso(mrconso_file, nrows=nrows)
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
        cuisty = self._load_sty(mrsty_file, nrows=nrows)
        curr_time = time.time()
        print(f'Loading/parsing semantic types: {curr_time - start} s')

        print('Writing semantic types...')
        start = time.time()
        self._dump_cuisty(cuisty, **kwargs)
        curr_time = time.time()
        print(f'Writing semantic types: {curr_time - start} s')

        t2 = time.time()
        print(f'Total runtime: {t2 - t1} s')
