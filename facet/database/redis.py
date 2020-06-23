import copy
import redis
from .base import BaseDatabase
from ..serializer import (
    serializer_map,
    BaseSerializer,
)
from typing import Union


__all__ = ['RedisDatabase']


class RedisDatabase(BaseDatabase):
    """Redis database interface.

    Args:
        host (str): See Redis documentation. Default is 'localhost'.

        port (int): See Redis documentation. Default is 6379.

        db (int): Database ID, see Redis documentation. Default is 0.

        pipe (bool): If set, queue 'set-related' commands to database.
            Run 'sync' command to submit commands in pipe.
            Default is False.

        serializer (str, BaseSerializer): Serializer instance or serializer
            name. Valid serializers are: 'json', 'yaml', 'pickle', 'string',
            'stringsj'. Default is 'json'.

    Kwargs:
        Options forwarded to 'Redis' class.

    Notes:
        * Redis treats keys/fields of 'str, bytes, and int'
          types interchangeably, but this interface accepts keys/fields
          of 'str' type and arbitrary values.

        * Redis Python API returns keys as 'bytes', so we use str.decode.
    """

    def __init__(
        self,
        host='localhost',
        *,
        port=6379,
        db=0,
        pipe=False,
        serializer: Union[str, 'BaseSerializer'] = 'json',
        **kwargs
    ):
        self._host = host
        self._port = port
        self._db_id = db

        self._serializer = None
        self.serializer = serializer

        # Connect to database
        self._db = redis.Redis(
            host=self._host,
            port=self._port,
            db=self._db_id,
            **kwargs,
        )

        # NOTE: Redis pipeline object is used only for 'set' operations
        # and requires invoking 'sync' to commit queued operations.
        self._dbp = None
        self._is_pipe = pipe
        self.set_pipe(pipe)

    def set_pipe(self, pipe):
        # NOTE: Invoke sync() when disabling pipe and pipe was enabled
        if not pipe:
            self.sync()
        self._is_pipe = pipe
        self._dbp = self._db.pipeline() if self._is_pipe else self._db

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def db_id(self):
        return self._db_id

    @property
    def config(self):
        info = copy.deepcopy(self._db.info())
        info.update(self._db.config_get())
        return info

    # @config.setter
    # def config(self, mapping):
    #     for key, value in mapping.items():
    #         self._db.config_set(key, value)

    @property
    def serializer(self):
        return self._serializer

    @serializer.setter
    def serializer(self, value: Union[str, 'BaseSerializer']):
        if isinstance(value, str):
            obj = serializer_map[value]()
        elif isinstance(value, BaseSerializer):
            obj = value
        else:
            raise ValueError(f'invalid serializer, {value}')
        self._serializer = obj

    def __iter__(self):
        return map(lambda key: key.decode(), self._db.scan_iter())

    def _get(self, key):
        try:
            value = self._db.get(key)
        except redis.exceptions.ResponseError:
            # NOTE: Assume key is a hash name, so return a field/value mapping.
            # Setting a hash map using this syntax is not supported
            # because it is ambiguous if a dictionary is the value or
            # the field/value mapping.
            return {
                field: self._hget(key, field) for field in self._hkeys(key)
            }
        if value is not None:
            value = self._serializer.loads(value)
        return value

    def _mget(self, keys):
        values = self._db.mget(keys)
        for i, value in enumerate(values):
            if value is not None:
                values[i] = self._serializer.loads(value)
            else:
                # NOTE: '_mget' returns None if key is a hash name, so
                # check if key is a hash name.
                values[i] = self._get(keys[i])
        return values

    def _hget(self, key, field):
        try:
            value = self._db.hget(key, field)
        except redis.exceptions.ResponseError:
            return None
        if value is not None:
            value = self._serializer.loads(value)
        return value

    def _hmget(self, key, fields):
        try:
            values = self._db.hmget(key, fields)
        except redis.exceptions.ResponseError:
            # NOTE: If key already exists and is not a hash name,
            # error is triggered. Return None for each field to have
            # the same behavior as when key does not exists.
            return len(fields) * [None]
        for i, value in enumerate(values):
            if value is not None:
                values[i] = self._serializer.loads(value)
        return values

    def _set(self, key, value, **kwargs):
        value = self._resolve_set(key, value, **kwargs)
        if value is not None:
            self._dbp.set(key, self._serializer.dumps(value))

    def _mset(self, mapping, **kwargs):
        _mapping = {}
        for key, value in mapping.items():
            value = self._resolve_set(key, value, **kwargs)
            if value is not None:
                _mapping[key] = self._serializer.dumps(value)
        if len(_mapping) > 0:
            self._dbp.mset(_mapping)

    def _hset(self, key, field, value, **kwargs):
        value = self._resolve_hset(key, field, value, **kwargs)
        if value is not None:
            value = self._serializer.dumps(value)
            try:
                self._dbp.hset(key, field, value)
            except redis.exceptions.ResponseError:
                # NOTE: Assume key is not a hash name.
                self._delete([key])
                self._dbp.hset(key, field, value)

    def _hmset(self, key, mapping, **kwargs):
        _mapping = {}
        for field, value in mapping.items():
            value = self._resolve_hset(key, field, value, **kwargs)
            if value is not None:
                _mapping[field] = self._serializer.dumps(value)
        if len(_mapping) > 0:
            self._dbp.hmset(key, _mapping)

    def _keys(self):
        return list(map(lambda key: key.decode(), self._db.keys()))

    def _hkeys(self, key):
        try:
            fields = self._db.hkeys(key)
        except redis.exceptions.ResponseError:
            # NOTE: Assume key is not a hash name.
            return []
        return list(map(lambda field: field.decode(), fields))

    def _len(self):
        return self._db.dbsize()

    def _hlen(self, key):
        try:
            return self._db.hlen(key)
        except redis.exceptions.ResponseError:
            # NOTE: Assume key is not a hash name.
            return 0

    def _exists(self, key):
        return bool(self._db.exists(key))

    def _hexists(self, key, field):
        try:
            return bool(self._db.hexists(key, field))
        except redis.exceptions.ResponseError:
            # NOTE: Assume key is not a hash name.
            return False

    def _delete(self, keys):
        for key in keys:
            self._db.delete(key)

    def _hdelete(self, key, fields):
        for field in filter(lambda f: self._hexists(key, f), fields):
            self._db.hdel(key, field)
        # NOTE: Delete key if it has no fields remaining
        if self._hlen(key) == 0:
            self._db.delete(key)

    def sync(self):
        if self._is_pipe:
            self._dbp.execute()

    def close(self):
        # NOTE: Redis object is disconnected automatically when object
        # goes out of scope.
        self._db = None
        self._dbp = None

    def clear(self):
        self._db.flushdb()

    def save(self, **kwargs):
        """Save Redis server data to disk (blocking operation)."""
        self._db.save()
