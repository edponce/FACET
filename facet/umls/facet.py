import os
import time
import itertools
from ..helpers import load_data
from unidecode import unidecode
from ..database import (
    database_map,
    BaseDatabase,
)
from ..base import BaseFacet
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
)
from .constants import (
    HEADERS_MRSTY,
    HEADERS_MRCONSO,
    ACCEPTED_SEMTYPES,
)


__all__ = ['UMLSFacet']


VERBOSE = True

# Enable/disable profiling
PROFILE = False
if PROFILE:
    import cProfile


class UMLSFacet(BaseFacet):
    """FACET text matcher.

    Args:
        conso_db (str, BaseDatabase): Handle to database instance or database
            name for CONCEPT-CUI storage. Valid database values are: 'dict',
            'redis', 'elasticsearch'.

        cuisty_db (str, BaseDatabase): Handle to database instance or database
            name for CUI-STY storage. Valid database values are: 'dict',
            'redis', 'elasticsearch'.
    """

    def __init__(
        self,
        *,
        conso_db: Union[str, 'BaseDatabase'] = None,
        cuisty_db: Union[str, 'BaseDatabase'] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._conso_db = None
        self._cuisty_db = None

        self.conso_db = conso_db
        self.cuisty_db = cuisty_db

    @property
    def conso_db(self):
        return self._conso_db

    @conso_db.setter
    def conso_db(self, value: Union[str, 'BaseDatabase']):
        if isinstance(value, str):
            obj = database_map[value]()
        elif value is None or isinstance(value, BaseDatabase):
            obj = value
        else:
            raise ValueError(f'invalid CONSO-CUI database, {value}')
        self._conso_db = obj

    @property
    def cuisty_db(self):
        return self._cuisty_db

    @cuisty_db.setter
    def cuisty_db(self, value: Union[str, 'BaseDatabase']):
        if isinstance(value, str):
            obj = database_map[value]()
        elif value is None or isinstance(value, BaseDatabase):
            obj = value
        else:
            raise ValueError(f'invalid CUI-STY database, {value}')
        self._cuisty_db = obj

    def _install(self, umls_dir: str, **kwargs):
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

        if self._conso_db is not None:
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

        if self._cuisty_db is not None:
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
        ngram_matches = []
        for candidate, similarity in self._simstring.search(
            ngram,
            **kwargs,
        ):
            ngram_match = {
                'begin': begin,
                'end': end,
                'ngram': ngram,
                'concept': candidate,
                'similarity': similarity,
            }

            if self._conso_db is None:
                ngram_matches.append(ngram_match)
                continue

            cui = self._conso_db.get(candidate)[0]
            if cui is None:
                continue

            ngram_match['CUI'] = cui

            if self._cuisty_db is None:
                ngram_matches.append(ngram_match)
                continue

            semtypes = self._cuisty_db.get(cui)
            if semtypes is None:
                continue

            ngram_match['semantic types'] = semtypes
            ngram_matches.append(ngram_match)

        return ngram_matches
