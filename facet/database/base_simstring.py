from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Iterable,
    Iterator,
    NoReturn,
)


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

        * Follows the framework design pattern - parent class controls
          the execution flow and subclass provides the details.
    """

    def __getitem__(
        self,
        key: Union[str, Iterable[str]],
    ) -> Union[List[Any], None, List[Union[List[Any], None]]]:
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> NoReturn:
        self.set(key, value)

    def __delitem__(self, key: str) -> NoReturn:
        self.delete(key)

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return self.len()

    def __enter__(self) -> 'BaseDatabase':
        return self

    def __exit__(self, type, value, traceback):
        self.save()
        self.close()

    @property
    def config(self) -> Dict[str, Any]:
        """Get configuraton information of database."""
        return {}

    def get(
        self,
        keys: Union[str, Iterable[str]],
        fields: Union[str, Iterable[str]] = None,
    ) -> Union[List[Any], None, List[Union[List[Any], None]]]:
        """Access values from database."""
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
        **kwargs
    ) -> NoReturn:
        """Insert values into database.

        Kwargs:
            replace (bool): If set, replace value, else add value to
                existing ones. If None, store value "as is". Default is None.

            unique (bool): If set, do not allow duplicate values.
                Used in conjunction with *replace* parameter.

        Notes:
              The second parameter is ambiguous between '_set' and
              '_hmset' when it represents a value that is a mapping.
              To resolve this, when value is a mapping use '_set'.
        """
        if isinstance(key_or_map, str):
            # 'key_or_map' is a key
            if value is None:
                if isinstance(val_or_field_or_map, dict):
                    # 'val_or_field_or_map' is a field/value mapping
                    self._hmset(key_or_map, val_or_field_or_map, **kwargs)
                else:
                    # 'val_or_field_or_map' is a value
                    self._set(key_or_map, val_or_field_or_map, **kwargs)
            else:
                # 'val_or_field_or_map' is a field
                self._hset(key_or_map, val_or_field_or_map, value, **kwargs)
        elif (isinstance(key_or_map, dict)
              and val_or_field_or_map is None
              and value is None):
            # 'key_or_map' is a key/value mapping
            self._mset(key_or_map, **kwargs)

    def keys(self, key: str = None) -> List[str]:
        """Get all keys in database."""
        return self._keys() if key is None else self._hkeys(key)

    def len(self, key: str = None) -> int:
        """Get numbers of keys in database or given key."""
        return self._len() if key is None else self._hlen(key)

    def items(self) -> Iterator[Tuple[str, List[Any]]]:
        """Get an iterator of key/value pairs."""
        for key in self._keys():
            yield key, self._get(key)

    def exists(self, key: str, field: str = None) -> bool:
        """Check if a key or field exist in the database."""
        return (
            self._exists(key) if field is None else self._hexists(key, field)
        )

    def delete(
        self,
        keys: Union[Iterable[str], str],
        fields: Iterable[str] = None,
    ) -> NoReturn:
        """Remove keys or fields from the database."""
        if fields is None:
            if isinstance(keys, str):
                keys = [keys]
            self._delete(keys)
        else:
            if isinstance(fields, str):
                fields = [fields]
            self._hdelete(keys, fields)

    def _resolve_set(
        self,
        key,
        value,
        *,
        replace=None,
        unique=False,
    ) -> Union[List[Any], None]:
        """Resolve final key/value to be used based on key/value's
        existence and value's uniqueness."""
        if replace is not None:
            if not replace and self._exists(key):
                prev_value = self._get(key)
                if isinstance(prev_value, dict):
                    prev_value = []
                elif unique and value in prev_value:
                    return None
                prev_value.append(value)
                value = prev_value
            else:
                value = [value]
        return value

    def _resolve_hset(
        self,
        key,
        field,
        value,
        *,
        replace=None,
        unique=False,
    ) -> Union[List[Any], None]:
        """Resolve final key/value to be used based on key/value's
        existence and value's uniqueness."""
        if replace is not None:
            if not replace and self._hexists(key, field):
                prev_value = self._hget(key, field)
                if prev_value is None:
                    prev_value = []
                elif unique and value in prev_value:
                    return None
                prev_value.append(value)
                value = prev_value
            else:
                value = [value]
        return value

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
        replace=None, unique=False,
    ) -> NoReturn:
        pass

    @abstractmethod
    def _mset(
        self, mapping: Dict[str, Any], *,
        replace=None, unique=False,
    ) -> NoReturn:
        pass

    @abstractmethod
    def _hset(
        self, key: str, field: str, value: Any, *,
        replace=None, unique=False,
    ) -> NoReturn:
        pass

    @abstractmethod
    def _hmset(
        self, key: str, mapping: Dict[str, Any], *,
        replace=None, unique=False,
    ) -> NoReturn:
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
    def _delete(self, keys: Iterable[str]) -> NoReturn:
        pass

    @abstractmethod
    def _hdelete(self, key: str, fields: Iterable[str]) -> NoReturn:
        pass

    def set_pipe(self, pipe: bool) -> NoReturn:
        """Enable/disable pipeline mode."""
        pass

    @abstractmethod
    def sync(self) -> NoReturn:
        """Submit queued commands."""
        pass

    @abstractmethod
    def close(self) -> NoReturn:
        """Close database connection."""
        pass

    @abstractmethod
    def clear(self) -> NoReturn:
        """Delete all keys in database."""
        pass

    @abstractmethod
    def save(self, **kwargs: Dict[str, Any]) -> NoReturn:
        """Save database."""
        pass
