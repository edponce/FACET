import collections
from .base import BaseDatabase
from typing import Union, List, Any


class DictDatabase(BaseDatabase):
    """Python dictionary database interface."""

    def __init__(self, **kwargs):
        self._db = collections.defaultdict(list)

    def _resolve_set(self, key, value,
                     replace=True,
                     unique=False) -> Union[List[Any], None]:
        if self._exists(key) and not replace:
            prev_value = self.get(key)
            if isinstance(prev_value, dict):
                prev_value = []
            elif unique and value in prev_value:
                return
            prev_value.append(value)
            value = prev_value
        else:
            value = [value]
        return value

    def _resolve_hset(self, key, field, value,
                      replace=True,
                      unique=False) -> Union[List[Any], None]:
        if self._hexists(key, field) and not replace:
            prev_value = self._hget(key, field)
            if prev_value is None:
                prev_value = []
            elif unique and value in prev_value:
                return
            prev_value.append(value)
            value = prev_value
        else:
            value = [value]
        return value

    def _is_hash_name(self, key: str) -> bool:
        return isinstance(self._get(key), defaultdict)

    def _get(self, key):
        try:
            return self._db[key]
        except KeyError:
            return

    def _mget(self, keys):
        return [self._get(key) for key in keys]

    def _hget(self, key, field):
        # if self._is_hash_name(key):
        #     return self._get

        value = self._get(key)
        if value is not None:
            value = self.serializer.loads(value)
        return value

    def set(self, key, value, **kwargs):
        value = self._resolve_set(key, value, **kwargs)
        if value is not None:
            self._db[key] = value

    def mset(self, mapping, **kwargs):
        _mapping = {}
        for key, value in mapping.items():
            value = self._resolve_set(key, value, **kwargs)
            if value is not None:
                _mapping[key] = value
        if len(_mapping) > 0:
            self._dbp.mset(_mapping)

    def hset(self, key, field, value, **kwargs):
        value = self._resolve_hset(key, value, **kwargs)
        if value is not None:
            self._dbp.hset(key, field, value)

    def hmget(self, key, fields):
        return list(map(
            self.serializer.loads,
            filter(lambda v: v is not None, self._db.hmget(key, fields))
        ))

    def hmset(self, key, mapping, **kwargs):
        _mapping = {}
        for field, value in mapping.items():
            value = self._resolve_hset(key, field, value, **kwargs)
            if value is not None:
                _mapping[field] = value
        if len(_mapping) > 0:
            self._dbp.hmset(key, _mapping)

    def keys(self):
        return self._db.keys()

    def hkeys(self, key):
        return list(map(self.serializer.loads, self._db.hkeys(key)))
        # NOTE: This approach does not work because an iterator is
        # returned and breaks the serializer.
        # yield self.serializer.loads(self._db.hscan_iter())

    def len(self):
        return len(self._db)

    def hlen(self, key):
        return len(self._db[key])

    def exists(self, key):
        return key in self._db

    def hexists(self, key, field):
        return field in self._db[key]

    def delete(self, keys):
        for key in keys:
            del self._db[key]

    def hdelete(self, key, fields):
        for field in fields:
            self._db[key].remove(field)

    def execute(self):
        pass

    def close(self):
        pass

    def flush(self):
        self._db = collections.defaultdict(list)
