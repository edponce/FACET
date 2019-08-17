from abc import ABC, abstractmethod
from typing import Any, List, Dict, Iterable
from QuickerUMLS.serializer import Serializer


class BaseDatabase(ABC):
    """Interface to database.

    Note:
        * Keys/fields should be of string datatype. Implementations of
          this interface should not need to validate keys/fields, it is
          up to the underlying database to handle or error accordingly.
          For example: Redis treats keys/fields of 'str, bytes, or int'
          types interchangeably.

        * Values should be serialized and deserialized by the
          implementation of this interface.
    """

    def __init__(self, **kwargs):
        self.serializer = kwargs.get('serializer', Serializer())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def get(self, key: str) -> List[Any]:
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: Any,
            extend=False, unique=False):
        """
        Args:
            extend (bool): If key exists, add value to existing values.
                Default is false.

            unique (bool): If set, do not allow duplicate values.
                Used in conjunction with `attr:extend`.
        """
        raise NotImplementedError

    @abstractmethod
    def mget(self, keys: Iterable[str]) -> List[List[Any]]:
        raise NotImplementedError

    @abstractmethod
    def mset(self, mapping: Dict[str, Any],
             extend=False, unique=False):
        """
        Args:
            extend (bool): If key exists, add value to existing values.
                Default is false.

            unique (bool): If set, do not allow duplicate values.
                Used in conjunction with `attr:extend`.
        """
        raise NotImplementedError

    @abstractmethod
    def hget(self, key: str, field: str) -> List[Any]:
        raise NotImplementedError

    @abstractmethod
    def hset(self, key: str, field: str, value: Any,
             extend=False, unique=False):
        """
        Args:
            extend (bool): If key exists, add value to existing values.
                Default is false.

            unique (bool): If set, do not allow duplicate values.
                Used in conjunction with `attr:extend`.
        """
        raise NotImplementedError

    @abstractmethod
    def hmget(self, key: str, fields: Iterable[str]) -> List[List[Any]]:
        raise NotImplementedError

    @abstractmethod
    def hmset(self, key: str, mapping: Dict[str, Any],
              extend=False, unique=False):
        """
        Args:
            extend (bool): If key exists, add value to existing values.
                Default is false.

            unique (bool): If set, do not allow duplicate values.
                Used in conjunction with `attr:extend`.
        """
        raise NotImplementedError

    @abstractmethod
    def keys(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def hkeys(self, key: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def hexists(self, key: str, field: str) -> bool:
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
