from abc import ABC, abstractmethod
from ..database import (
    database_map,
    BaseDatabase,
)
from typing import (
    List,
    Tuple,
    Union,
)


__all__ = ['BaseMatcher']


class BaseMatcher(ABC):
    """Interface for implementations of matching algorithms.

    Args:
        db (str, BaseDatabase): Handle to database instance or database name
            for string matching store. Valid databases are: 'dict', 'redis',
            'sqlite'.

        cache_db (str, BaseDatabase): Handle to database instance or database
            name for caching string matches. Valid databases are: 'dict',
            'redis', 'sqlite'.
    """

    def __init__(
        self,
        *,
        db: Union[str, 'BaseDatabase'] = None,
        cache_db: Union[str, 'BaseDatabase'] = None,
    ):
        self._db = None
        self._cache_db = None
        self.db = db
        self.cache_db = cache_db

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, value: Union[str, 'BaseDatabase']):
        if isinstance(value, str):
            obj = database_map[value]()
        elif value is None or isinstance(value, BaseDatabase):
            obj = value
        else:
            raise ValueError(f'invalid matcher database, {value}')
        self._db = obj

    @property
    def cache_db(self):
        return self._cache_db

    @cache_db.setter
    def cache_db(self, value: Union[str, 'BaseDatabase']):
        if isinstance(value, str):
            obj = database_map[value]()
        elif value is None or isinstance(value, BaseDatabase):
            obj = value
        else:
            raise ValueError(f'invalid matcher cache database, {value}')
        self._cache_db = obj

    def close(self):
        if self._cache_db is not None:
            self._cache_db.close()
        if self._db is not None:
            self._db.close()

    @abstractmethod
    def insert(self, string: str):
        pass

    @abstractmethod
    def search(
        self,
        query_string: str,
        **kwargs,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        pass
