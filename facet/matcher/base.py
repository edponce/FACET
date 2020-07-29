from abc import ABC, abstractmethod
from ..database import (
    get_database,
    BaseDatabase,
)
from typing import (
    Any,
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
        self._tmp_db = None

    @property
    def db(self):
        return self._db

    # NOTE: Need to rethink these proxy database and transformations.
    # Think about the case where database serializer does not supports
    # a value type (e.g., YAML/JSON with sets), but sets provide improvements
    # for Simstring database accesses. For now, only support pickle
    # serializer.
    def set_proxy_db(self, db: Union[None, 'BaseDatabase']):
        """Toggle proxy database."""
        if db is None:
            if self._tmp_db is not None:
                # self._transform_proxy_db()
                self._db.copy(self._tmp_db)
                self._db.clear()
                self._db, self._tmp_db = self._tmp_db, None
        else:
            self._tmp_db, self._db = self._db, get_database(db)

    def _transform_proxy_db(self, value: Any):
        """Operations to perform to proxy database before copying into
        matcher's database."""
        pass

    @property
    def cache_db(self):
        return self._cache_db

    @abstractmethod
    def insert(self, string: str):
        pass

    @abstractmethod
    def search(self, string: str) -> Union[List[Tuple[str, float]], List[str]]:
        pass
