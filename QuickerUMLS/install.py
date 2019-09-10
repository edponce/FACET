import os
import sys
import time
import collections
from unidecode import unidecode
from QuickerUMLS.simstring import Simstring
from QuickerUMLS.database import DictDatabase
from QuickerUMLS.helpers import (
    # data_to_dict,
    iter_data,
    is_iterable,
)
from QuickerUMLS.umls_constants import (
    HEADERS_MRCONSO,
    HEADERS_MRSTY,
    ACCEPTED_SEMTYPES,
)
from typing import (
    Any,
    Union,
    List,
    Dict,
    Callable,
    Iterable,
    Generator,
    NoReturn,
)


__all__ = ['FACET']


VERBOSE = True


class FACET:
    """FACET UMLS installation tool.

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
        conso_db: 'BaseDatabase' = DictDatabase(),
        cuisty_db: 'BaseDatabase' = DictDatabase(),
        simstring: 'Simstring' = Simstring(),
    ):
        self._conso_db = conso_db
        self._cuisty_db = cuisty_db
        self._ss = simstring

    def _load_conso(self, afile: str, **kwargs) -> Generator:
        def configure_converters() -> Dict[str, List[Callable]]:
            """Converter functions are used to process dataset during load
            operations.

            Converter functions should only take a single parameter
            (or use lambda and preset extra parameters).
            """
            converters = collections.defaultdict(list)
            # Optional
            if 'str' not in converters:
                converters['str'].append(str.lower)
                converters['str'].append(unidecode)
            # Permanent
            if 'ispref' not in converters:
                converters['ispref'].append(lambda v: True if v == 'Y' else False)
            return converters

        language = kwargs.get('language', ['ENG'])
        if not is_iterable(language):
            language = [language]

        # NOTE: This version is returns a dictionary data structure
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
            nrows=kwargs.get('nrows', None),
        )

    def _load_sty(
        self,
        afile: str,
        *,
        conso: Dict[Any, Any] = None,
        **kwargs
    ) -> Generator:
        valids={'sty': ACCEPTED_SEMTYPES}
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
            nrows=kwargs.get('nrows', None),
        )

    def _dump_conso(
        self,
        data: Iterable[Any],
        *,
        bulk_size: int = 1000,
        status_step: int = 10000,
    ) -> NoReturn:
        """Stores Term-CUI,Preferred mapping, term: [(CUI,pref), ...]."""
        # Profile
        prev_time = time.time()

        for i, ((term, cui), preferred) in enumerate(data, start=1):
            self._ss.insert(term)
            self._conso_db.set(term, (cui, preferred), replace=False, unique=True)

            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                print(f'{i}: {curr_time - prev_time} s, '
                      f'{(curr_time - prev_time) / (bulk_size * (status_step / bulk_size))} s/batch')
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
        """Stores CUI-Semantic Type mapping, cui: [sty, ...]."""
        # Profile
        prev_time = time.time()

        for i, (cui, sty) in enumerate(data, start=1):
            self._cuisty_db.set(cui, sty, replace=False, unique=True)

            if VERBOSE and i % status_step == 0:
                curr_time = time.time()
                print(f'{i}: {curr_time - prev_time} s, '
                      f'{(curr_time - prev_time) / (bulk_size * (status_step / bulk_size))} s/batch')
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
            language (Union[str, List[str]]): Extract concepts of the
                specified languages. Default is 'ENG'.

            bulk_size (int): Size of chunks to use for dumping data into
                databases. Default is 1000.

            status_step (int): Print status message after this number of
                records is dumped to databases. Default is 10000.
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

    def match(self):
        pass
