from abc import ABC, abstractmethod
from typing import Any, List, Dict, Tuple, Union, Iterable, Iterator


__all__ = ['BaseDatabase']


class BaseDatabase(ABC):
    """Interface to key/value store database.

    Notes:
        * Keys/fields should be of string datatype. Implementations of
          this interface should not need to validate keys/fields, it is
          up to the underlying database to handle or error accordingly.

        * If needed, values should be serialized and deserialized by the
          implementation of this interface.

        * All values are represented as either lists or dictionaries.
          Dictionaries exclusively represent field/values corresponding
          to a hash map.
    """

    def __getitem__(
        self,
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

    def __exit__(self, *exc_info):
        self.close()

    def get(
        self,
        keys: Union[str, Iterable[str]],
        fields: Union[str, Iterable[str]] = None,
    ) -> Union[List[Any], None, List[Union[List[Any], None]]]:
        result = None
        if isinstance(keys, str):
            if fields is None:
                result = self._get(keys)
            elif isinstance(fields, str):
                result = self._hget(keys, fields)
            else:
                # Assume 'fields' is an iterable
                result = self._hmget(keys, fields)
        elif fields is None:
            # Assume 'keys' is an iterable
            result = self._mget(keys)
        return result

    def set(
        self,
        key_or_map: Union[str, Dict[str, Any]],
        val_or_field_or_map: Union[Any, str, Dict[str, Any]] = None,
        value: Any = None,
        *,
        replace=True, unique=False,
    ):
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
                    self._hmset(key_or_map, val_or_field_or_map)
                else:
                    self._set(key_or_map, val_or_field_or_map)
            else:
                # Assume 'val_or_field_or_map' is a field
                self._hset(key_or_map, val_or_field_or_map, value)
        elif (isinstance(key_or_map, dict)
              and val_or_field_or_map is None
              and value is None):
            self._mset(key_or_map)

    def keys(self, key: str = None) -> List[str]:
        return self._keys() if key is None else self._hkeys(key)

    def len(self, key: str = None) -> int:
        return self._len() if key is None else self._hlen(key)

    def items(self) -> Iterator[Tuple[str, List[Any]]]:
        for key in self._keys():
            yield key, self._get(key)

    def exists(self, key: str, field: str = None) -> bool:
        return (
            self._exists(key) if field is None else self._hexists(key, field)
        )

    def delete(
        self,
        keys: Union[Iterable[str], str],
        fields: Iterable[str] = None,
    ):
        if fields is None:
            self._delete(keys)
        else:
            self._hdelete(keys, fields)

    @abstractmethod
    def _get(self, key: str) -> Union[List[Any], None]:
        pass

    @abstractmethod
    def _mget(self, keys: Iterable[str]) -> List[Union[List[Any], None]]:
        pass

    @abstractmethod
    def _hget(self, key: str, field: str) -> Union[List[Any], None]:
        pass

    @abstractmethod
    def _hmget(
        self,
        key: str,
        fields: Iterable[str]
    ) -> List[Union[List[Any], None]]:
        pass

    @abstractmethod
    def _set(
        self, key: str, value: Any, *,
        replace=True, unique=False,
    ):
        pass

    @abstractmethod
    def _mset(
        self, mapping: Dict[str, Any], *,
        replace=True, unique=False,
    ):
        pass

    @abstractmethod
    def _hset(
        self, key: str, field: str, value: Any, *,
        replace=True, unique=False,
    ):
        pass

    @abstractmethod
    def _hmset(
        self, key: str, mapping: Dict[str, Any], *,
        replace=True, unique=False,
    ):
        pass

    @abstractmethod
    def _keys(self) -> List[str]:
        pass

    @abstractmethod
    def _hkeys(self, key: str) -> List[str]:
        pass

    @abstractmethod
    def _len(self) -> int:
        """Returns the total number of keys in database."""
        pass

    @abstractmethod
    def _hlen(self, key: str) -> int:
        """Return number of fields in hash map.
        If key is not a hash name, then return 0
        If key does not exists, then return 0.
        """
        pass

    @abstractmethod
    def _exists(self, key: str) -> bool:
        pass

    @abstractmethod
    def _hexists(self, key: str, field: str) -> bool:
        """Check existence of a hash name.
        If key is not a hash name, then return false.
        """
        pass

    @abstractmethod
    def _delete(self, keys: Iterable[str]):
        pass

    @abstractmethod
    def _hdelete(self, key: str, fields: Iterable[str]):
        pass

    @abstractmethod
    def sync(self):
        """Submit queued commands.
        Not all databases support this functionality.
        """
        pass

    @abstractmethod
    def close(self):
        """Close database connection."""
        pass

    @abstractmethod
    def clear(self):
        """Delete all keys in database."""
        pass

    @abstractmethod
    def save(self, **kwargs):
        pass

    def config(
        self,
        mapping: Dict[str, Any] = {},
    ) -> Union[Dict[str, Any], None]:
        """Get/set configuration of database.
        If mapping is empty, return configuration, else set configuration.
        """
        pass
