import os
import pickle
import shelve
import collections
from .base import BaseDatabase
from typing import Union, List, Any


__all__ = ['DictDatabase']


class DictDatabase(BaseDatabase):
    """Python dictionary database interface.

    Args:
        db_file (str): Database filename.

        pipe (bool): If set, queue 'set-related' commands to cached database.
            Run 'sync' command to submit commands in pipe.
            Default is False.

        flag (str): Database open mode. Valid modes are 'r' = read,
            'w' = write, 'c' = read/write/create, and 'n' = new.
            Default is 'c'. Additional characters can be appended to the
            flag for more control. Valid values are 'f' = fast/no sync,
            's' = sync, and 'u' = lock.

        kwargs (Dict[str, Any]): Option forwarding, see 'pickle'.
            E.g., 'protocol' and 'encoding'.

    Notes:
        * The underyling database is managed by a persistent dictionary
          and values are serialized by 'pickle'.

        * Keys/fields are treated as ordinary 'str'.
    """

    def __init__(self, db_file, **kwargs):
        self._db_file = db_file
        self._is_pipe = kwargs.get('pipe', False)
        if 'protocol' not in kwargs:
            kwargs['protocol'] = pickle.HIGHEST_PROTOCOL

        # Connect to database
        self._db = shelve.open(
            self._db_file,
            writeback=self._is_pipe,
            **kwargs,
        )

    @property
    def db_file(self):
        return self._db_file

    @property
    def is_pipe(self):
        return self._is_pipe

    def _is_hash_name(self, key: str) -> bool:
        """Detect if a key is a hash name.
        Hash maps use a 'defaultdict' for representing fields/values.
        """
        return isinstance(self._db[key], dict) if self._exists(key) else False

    def _get(self, key):
        return self._db[key] if self._exists(key) else None

    def _mget(self, keys):
        return [self._get(key) for key in keys]

    def _hget(self, key, field):
        return self._db[key][field] if self._is_hash_name(key) else None

    def _hmget(self, key, fields):
        values = len(fields) * [None]
        if self._is_hash_name(key):
            mapping = self._db[key]
            for i, field in enumerate(fields):
                if field in mapping:
                    values[i] = mapping[field]
        return values

    def _resolve_set(self, key, value,
                     replace=True,
                     unique=False) -> Union[List[Any], None]:
        """Resolve final key/value to be used based on key/value's
        existence and value's uniqueness."""
        if self._exists(key) and not replace:
            prev_value = self.get(key)
            if isinstance(prev_value, dict):
                prev_value = []
            elif unique and value in prev_value:
                return None
            prev_value.append(value)
            value = prev_value
        else:
            value = [value]
        return value

    def _resolve_hset(self, key, field, value,
                      replace=True,
                      unique=False) -> Union[List[Any], None]:
        """Resolve final key/value to be used based on key/value's
        existence and value's uniqueness."""
        if self._hexists(key, field) and not replace:
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

    def _set(self, key, value, **kwargs):
        value = self._resolve_set(key, value, **kwargs)
        if value is not None:
            self._db[key] = value

    def _mset(self, mapping, **kwargs):
        for key, value in mapping.items():
            value = self._resolve_set(key, value, **kwargs)
            if value is not None:
                self._db[key] = value

    def _hset(self, key, field, value, **kwargs):
        value = self._resolve_hset(key, field, value, **kwargs)
        if value is not None:
            self._db[key] = {field: value}

    def _hmset(self, key, mapping, **kwargs):
        _mapping = {}
        for field, value in mapping.items():
            value = self._resolve_hset(key, field, value, **kwargs)
            if value is not None:
                _mapping[field] = value
        if len(_mapping) > 0:
            self._db[key] = _mapping

    def _keys(self):
        return list(self._db.keys())

    def _hkeys(self, key):
        fields = []
        if self._exists(key):
            mapping = self._db[key]
            if isinstance(mapping, dict):
                fields = self._db[key].keys()
        return list(fields)

    def _len(self):
        return len(self._db)

    def _hlen(self, key):
        _len = 0
        if self._exists(key):
            mapping = self._db[key]
            if isinstance(mapping, dict):
                _len = len(mapping)
        return _len

    def _exists(self, key):
        return key in self._db

    def _hexists(self, key, field):
        valid = False
        if self._exists(key):
            mapping = self._db[key]
            if isinstance(mapping, dict) and field in mapping:
                valid = True
        return valid

    def _delete(self, keys):
        for key in keys:
            if self._exists(key):
                del self._db[key]

    def _hdelete(self, key, fields):
        for field in fields:
            if self._hexists(key, field):
                del self._db[key][field]

    def sync(self):
        if self._is_pipe:
            self._db.sync()

    def close(self):
        self._db.close()

    def clear(self):
        self._db.clear()

    save = sync
