from .ngram import CharacterFeatures
from .similarity import CosineSimilarity
from typing import Union, List, NoReturn
from QuickerUMLS.database import DictDatabase


__all__ = ['Simstring']


class Simstring:

    def __init__(self, **kwargs):
        self._db = kwargs.get('database', DictDatabase())
        self._fe = kwargs.get('feature_extractor', CharacterFeatures())
        self._measure = kwargs.get('similarity', CosineSimilarity())
        self._cache_enabled = kwargs.get('enable_cache', False)

        # NOTE: Keep a multilevel dictionary in memory containing
        # the most recent lookups. Do not use cache if database is an
        # in-memory dictionary.
        if isinstance(self._db, DictDatabase):
            self._max_cache_size = 0
            self._cache_db = None
            self._cache_enabled = False
        elif self._cache_enabled:
            self._max_cache_size = kwargs.get('max_cache_size', 100)
            self._cache_db = DictDatabase()
            self._cache_enabled = True
        self._cache_size = 0

    @property
    def db(self):
        # NOTE: Allow access to database so that methods of interest
        # can be controlled externally. For example, 'sync' (for
        # pipe-enabled databases), 'close', and 'save'.
        return self._db

    def insert(self, string: str) -> NoReturn:
        """Insert string into database."""
        features = [f.lower() for f in self._fe.get_features(string)]
        self._db.set(
            str(len(features)),
            # NOTE: Create set here to remove duplicates and not
            # affect the numbers of features extracted.
            {feature: string for feature in set(features)},
            replace=False,
            unique=True
        )

    def _lookup_strings(
        self,
        size: Union[int, str],
        feature: str,
    ) -> List[str]:
        """Get string corresponding to feature size and query feature."""
        _size = str(size)
        if self._cache_enabled:
            if self._cache_db.exists(_size, feature):
                string = self._cache_db.get(_size, feature)
            else:
                string = self._db.get(_size, feature)
                if string is not None:
                    self._cache_string(_size, feature, string)
        else:
            string = self._db.get(_size, feature)
        return string

    def _cache_string(
        self,
        size: Union[int, str],
        feature: str,
        string: str,
    ) -> NoReturn:
        _size = str(size)
        self._cache_db.set(_size, feature, string, replace=False, unique=True)
        # NOTE: Need to limit cache size, which items to remove?
        self._cache_size += 1
