from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Iterable, Iterator


class BaseDatabase(ABC):
    """Interface to database.

    Notes:
        * Keys/fields should be of string datatype. Implementations of
          this interface should not need to validate keys/fields, it is
          up to the underlying database to handle or error accordingly.

        * If needed, values should be serialized and deserialized by the
          implementation of this interface.

        * All values are represented as lists.
    """

    def __getitem__(self,
                    key: Union[str, Iterable[str]],
                    ) -> Union[List[Any], None, List[Union[List[Any], None]]]:
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

    def get(self,
            keys: Union[str, Iterable[str]],
            fields: Union[str, Iterable[str]] = None
            ) -> Union[List[Any], None, List[Union[List[Any], None]]]:
        if isinstance(keys, str):
            if fields is None:
                return self._get(keys)
            elif isinstance(fields, str):
                return self._hget(keys, fields)
            else:
                # Assume 'fields' is an iterable
                return self._hmget(keys, fields)
        elif fields is None:
            # Assume 'keys' is an iterable
            return self._mget(keys)

    @abstractmethod
    def _get(self,
             key: str
             ) -> Union[List[Any], None]:
        raise NotImplementedError

    @abstractmethod
    def _mget(self,
              keys: Iterable[str]
              ) -> List[Union[List[Any], None]]:
        raise NotImplementedError

    @abstractmethod
    def _hget(self,
              key: str,
              field: str
              ) -> Union[List[Any], None]:
        raise NotImplementedError

    @abstractmethod
    def _hmget(self,
               key: str,
               fields: Iterable[str]
               ) -> List[Union[List[Any], None]]:
        raise NotImplementedError

    def set(self,
            key_or_map: Union[str, Dict[str, Any]],
            val_or_field_or_map: Union[Any, str, Dict[str, Any]] = None,
            value: Any = None,
            *,
            replace=True, unique=False):
        """
        Args:
            replace (bool): If set, replace value, else add value to
                existing ones. Default is true.

            unique (bool): If set, do not allow duplicate values.
                Used in conjunction with `attr:replace`.

        Notes:
              The second parameter is ambiguous between '_set' and
              '_hmset' when it represents a value that is a mapping.
              To resolve this, when value is a mapping use '_set'.
        """
        if isinstance(key_or_map, str):
            if value is None:
                if isinstance(val_or_field_or_map, dict):
                    # Assume 'value' is a mapping
                    return self._hmset(key_or_map, val_or_field_or_map)
                else:
                    return self._set(key_or_map, val_or_field_or_map)
            else:
                # Assume 'val_or_field_or_map' is a field
                return self._hset(key_or_map, val_or_field_or_map, value)
        elif (isinstance(key_or_map, dict)
              and val_or_field_or_map is None
              and value is None):
            return self._mset(key_or_map)

    @abstractmethod
    def _set(self, key: str, value: Any, *,
             replace=True, unique=False):
        raise NotImplementedError

    @abstractmethod
    def _mset(self, mapping: Dict[str, Any], *,
              replace=True, unique=False):
        raise NotImplementedError

    @abstractmethod
    def _hset(self, key: str, field: str, value: Any, *,
              replace=True, unique=False):
        raise NotImplementedError

    @abstractmethod
    def _hmset(self, key: str, mapping: Dict[str, Any], *,
               replace=True, unique=False):
        raise NotImplementedError

    def keys(self, key: str = None) -> List[str]:
        if key is None:
            return self._keys()
        else:
            return self._hkeys(key)

    @abstractmethod
    def _keys(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def _hkeys(self, key: str) -> List[str]:
        raise NotImplementedError

    def len(self, key: str = None) -> int:
        if key is None:
            return self._len()
        else:
            return self._hlen(key)

    @abstractmethod
    def _len(self) -> int:
        """Returns the total number of keys in database."""
        raise NotImplementedError

    @abstractmethod
    def _hlen(self, key: str) -> int:
        """Return number of fields in hash map.
        If key is not a hash name, then return 0
        If key does not exists, then return 0.
        """
        raise NotImplementedError

    def exists(self, key: str, field: str = None) -> bool:
        if field is None:
            return self._exists(key)
        else:
            return self._hexists(key, field)

    @abstractmethod
    def _exists(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _hexists(self, key: str, field: str) -> bool:
        """
        If key is not a hash name, then return false.
        """
        raise NotImplementedError

    def delete(self,
               keys: Union[Iterable[str], str],
               fields: Iterable[str] = None):
        if fields is None:
            self._delete(keys)
        else:
            self._hdelete(keys, fields)

    @abstractmethod
    def _delete(self, keys: Iterable[str]):
        raise NotImplementedError

    @abstractmethod
    def _hdelete(self, key: str, fields: Iterable[str]):
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
    def save(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def config(self,
               mapping: Dict[str, Any] = {}) -> Union[Dict[str, Any], None]:
        """Get/set configuration of database.
        If mapping is empty, return configuration, else set configuration.
        """
        raise NotImplementedError
