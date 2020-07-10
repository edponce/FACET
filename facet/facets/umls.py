import os
import time
from unidecode import unidecode
from ..utils import load_data
from ..database import (
    database_map,
    BaseDatabase,
)
from .base import BaseFacet
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Iterable,
)


__all__ = ['UMLSFacet']


VERBOSE = True


# NOTE: UMLS headers should be automatically parsed from UMLS MRFILES.RRF.
HEADERS_MRCONSO = (
    'cui', 'lat', 'ts', 'lui', 'stt', 'sui', 'ispref', 'aui', 'saui',
    'scui', 'sdui', 'sab', 'tty', 'code', 'str', 'srl', 'suppress', 'cvf'
)

# NOTE: UMLS headers should be automatically parsed from UMLS MRFILES.RRF.
HEADERS_MRSTY = (
    'cui', 'sty', 'hier', 'desc', 'sid', 'num'
)

ACCEPTED_SEMTYPES = {
    'T029': 'Body Location or Region',
    'T023': 'Body Part, Organ, or Organ Component',
    'T031': 'Body Substance',
    'T060': 'Diagnostic Procedure',
    'T047': 'Disease or Syndrome',
    'T074': 'Medical Device',
    'T200': 'Clinical Drug',
    'T203': 'Drug Delivery Device',
    'T033': 'Finding',
    'T184': 'Sign or Symptom',
    'T034': 'Laboratory or Test Result',
    'T058': 'Health Care Activity',
    'T059': 'Laboratory Procedure',
    'T037': 'Injury or Poisoning',
    'T061': 'Therapeutic or Preventive Procedure',
    'T048': 'Mental or Behavioral Dysfunction',
    'T046': 'Pathologic Function',
    'T121': 'Pharmacologic Substance',
    'T201': 'Clinical Attribute',
    'T130': 'Indicator, Reagent, or Diagnostic Aid',
    'T195': 'Antibiotic',
    'T039': 'Physiologic Function',
    'T040': 'Organism Function',
    'T041': 'Mental Process',
    'T170': 'Intellectual Product',
    'T191': 'Neoplastic Process'
}


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

    def _install(
        self,
        data: str,
        *,
        cui_valids: Dict[str, Iterable[Any]] = None,
        sty_valids: Dict[str, Iterable[Any]] = {'sty': ACCEPTED_SEMTYPES},
        **kwargs,
    ):
        """UMLS installation that minimizes database storage footprint
        but requires more runtime memory during installation.

        Database storage is less because CONCEPT-CUI and CUI-STY tables
        are joined based on CUIs and ACCEPTED_SEMTYPES.

        Args:
            data (str): Directory of UMLS RRF files.

        Kwargs:
            Options passed directly to '*load_data()' function.
        """
        t1 = time.time()

        # NOTE: This allows user configuration to use either None or {}
        # for disabling these filters.
        if cui_valids is None:
            cui_valids = {}
        if sty_valids is None:
            sty_valids = {}

        # NOTE: Even if 'conso_db' is None, we can filter based on semantic
        # types selected.
        if self._cuisty_db is not None:
            print('Loading/parsing semantic types...')
            start = time.time()
            mrsty_file = os.path.join(data, 'MRSTY.RRF')
            cuisty = load_data(
                mrsty_file,
                keys=['cui'],
                values=['sty'],
                headers=HEADERS_MRSTY,
                valids={**cui_valids, **sty_valids},
                multiple_values=True,
                unique_values=True,
                delimiter='|',
                **kwargs,
            )
            curr_time = time.time()
            print(f'Loading/parsing semantic types: {curr_time - start} s')

            print('Writing semantic types...')
            start = time.time()
            # Stores {CUI:Semantic Type} mapping, cui: [sty, ...]
            self._dump_kv(cuisty.items(), db=self._cuisty_db)
            curr_time = time.time()
            print(f'Writing semantic types: {curr_time - start} s')

            # Join tables based on CUIs
            if len(cui_valids) == 0:
                cui_valids = {'cui': cuisty.keys()}

        print('Loading/parsing concepts...')
        start = time.time()
        mrconso_file = os.path.join(data, 'MRCONSO.RRF')
        conso = load_data(
            mrconso_file,
            keys=['str'],
            values=['cui'] if self._conso_db is not None else None,
            headers=HEADERS_MRCONSO,
            valids={**cui_valids, **{'lat': ['ENG']}},
            converters={'str': [unidecode, str.lower]},
            delimiter='|',
            **kwargs,
        )
        curr_time = time.time()
        print(f'Loading/parsing concepts: {curr_time - start} s')

        if self._conso_db is not None:
            print('Writing concepts and matcher data...')
            start = time.time()
            # Stores {Term:CUI} mapping, term: [CUI, ...]
            self._dump_matcher_kv(conso.items(), db=self._conso_db)
            curr_time = time.time()
            print(f'Writing concepts and matcher data: {curr_time - start} s')

        else:
            # NOTE: List when 'load_data(values=None)' does not have an
            # argument for 'values'.
            print('Writing matcher data...')
            start = time.time()
            # Stores Matcher-specific data
            self._dump_matcher(conso)
            curr_time = time.time()
            print(f'Writing matcher data: {curr_time - start} s')

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
            Options passed directly to `Matcher.search()`.
        """
        begin, end, ngram = ngram_struct
        ngram_matches = []
        for candidate, similarity in self._matcher.search(
            ngram,
            **kwargs,
        ):
            ngram_match = {
                'begin': begin,
                'end': end,
                'ngram': ngram,
                'candidate': candidate,
                'similarity': similarity,
            }

            if self._conso_db is not None:
                cui = self._conso_db.get(candidate)
                ngram_match['CUI'] = cui
                if cui is not None and self._cuisty_db is not None:
                    ngram_match['semantic types'] = self._cuisty_db.get(cui)
            ngram_matches.append(ngram_match)

        return ngram_matches

    def _close(self):
        if self._conso_db is not None:
            self._conso_db.close()
        if self._cuisty_db is not None:
            self._cuisty_db.close()
