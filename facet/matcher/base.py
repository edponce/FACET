from abc import ABC, abstractmethod
from ..database import (
    get_database,
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
            for string matching store.

        cache_db (str, BaseDatabase): Handle to database instance or database
            name for caching string matches.
    """

    def __init__(
        self,
        *,
        db: Union[str, 'BaseDatabase'] = None,
        cache_db: Union[str, 'BaseDatabase'] = None,
    ):
        self._db = get_database(db)
        self._cache_db = get_database(cache_db)

    @property
    def db(self):
        return self._db

    @property
    def cache_db(self):
        return self._cache_db

    @abstractmethod
    def insert(self, string: str):
        pass

    @abstractmethod
    def search(self, string: str) -> Union[List[Tuple[str, float]], List[str]]:
        pass
