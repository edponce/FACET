from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Iterable, Iterator


class BaseDatabase(ABC):
    """Interface to database.

    Note:
        * Keys/fields should be of string datatype. Implementations of
          this interface should not need to validate keys/fields, it is
          up to the underlying database to handle or error accordingly.

        * If needed, values should be serialized and deserialized by the
          implementation of this interface.

        * All values are represented as lists.
    """

    def __getitem__(self, key: str) -> Union[List[Any], None]:
        return self.get(key)

    def __setitem__(self, key: str, value: Any):
        self.set(key, value)

    def __delitem__(self, key: str):
        self.delete([key])

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return self.len()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def get(self, key: str) -> Union[List[Any], None]:
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: Any, *,
            replace=True, unique=False):
        """
        Args:
            replace (bool): If set, replace value, else add value to
                existing ones. Default is true.

            unique (bool): If set, do not allow duplicate values.
                Used in conjunction with `attr:replace`.
        """
        raise NotImplementedError

    @abstractmethod
    def mget(self, keys: Iterable[str]) -> List[Union[List[Any], None]]:
        raise NotImplementedError

    @abstractmethod
    def mset(self, mapping: Dict[str, Any], *,
             replace=True, unique=False):
        raise NotImplementedError

    @abstractmethod
    def hget(self, key: str, field: str) -> Union[List[Any], None]:
        raise NotImplementedError

    @abstractmethod
    def hset(self, key: str, field: str, value: Any, *,
             replace=True, unique=False):
        raise NotImplementedError

    @abstractmethod
    def hmget(self, key: str,
              fields: Iterable[str]) -> List[Union[List[Any], None]]:
        raise NotImplementedError

    @abstractmethod
    def hmset(self, key: str, mapping: Dict[str, Any], *,
              replace=True, unique=False):
        raise NotImplementedError

    @abstractmethod
    def keys(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def hkeys(self, key: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def len(self) -> int:
        """Returns the total number of keys in database."""
        raise NotImplementedError

    @abstractmethod
    def hlen(self, key: str) -> int:
        """Return number of fields in hash map.
        If key is not a hash name, then return 0
        If key does not exists, then return 0.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def hexists(self, key: str, field: str) -> bool:
        """
        If key is not a hash name, then return false.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, keys: Iterable[str]):
        raise NotImplementedError

    @abstractmethod
    def hdelete(self, key: str, fields: Iterable[str]):
        raise NotImplementedError

    @abstractmethod
    def execute(self):
        """Submit queued commands.
        Not all databases support this functionality."""
        raise NotImplementedError

    @abstractmethod
    def close(self):
        """Close database connection."""
        raise NotImplementedError

    @abstractmethod
    def flush(self):
        """Delete all keys in database."""
        raise NotImplementedError

    @abstractmethod
    def save(self):
        raise NotImplementedError

    @abstractmethod
    def config(self,
               mapping: Dict[str, Any] = {}) -> Union[Dict[str, Any], None]:
        """Get/set configuration of database.
        If mapping is empty, return configuration, else set configuration.
        """
        raise NotImplementedError
