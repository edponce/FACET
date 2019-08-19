import redis
from .base import BaseDatabase
from typing import Union, List, Any
from QuickerUMLS.serializer import Serializer


class RedisDatabase(BaseDatabase):
    """Redis database interface.

    Args:
        host (str): See Redis documentation. Default is 'localhost'.

        port (int): See Redis documentation. Default is 6379.

        db (int): Database ID, see Redis documentation. Default is 0.

        pipe (bool): If set, queue 'set-related' commands to Redis database.
            Run 'execute' command to submit commands in pipe.
            Default is False.

        kwargs (Dict[str, Any]): Option forwaring, see `class:Serializer`.

    Notes:
        * Redis treats keys/fields of 'str, bytes, and int'
          types interchangeably.
    """

    def __init__(self, **kwargs):
        self._host = kwargs.get('host', 'localhost')
        self._port = kwargs.get('port', 6379)
        self._db_id = kwargs.get('db', 0)
        self._is_pipe = kwargs.get('pipe', False)
        self._db = redis.Redis(
            host=self._host,
            port=self._port,
            db=self._db_id,
        )
        # NOTE: Redis pipeline object is used only for 'set' operations
        # and requires invoking 'execute' to commit queued operations.
        self._dbp = self._db.pipeline() if self._is_pipe else self._db

        self.serializer = kwargs.get('serializer', Serializer(**kwargs))

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
    def is_pipe(self):
        return self._is_pipe

    def __iter__(self):
        return (self.serializer.loads(key) for key in self._db.scan_iter())

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
            return self.serializer.loads(value)

    def _mget(self, keys):
        values = self._db.mget(keys)
        for i, value in enumerate(values):
            if value is not None:
                values[i] = self.serializer.loads(value)
            else:
                # NOTE: '_mget' returns None if key is a hash name, so
                # check if key is a hash name.
                values[i] = self._get(keys[i])
        return values

    def _hget(self, key, field):
        try:
            value = self._db.hget(key, field)
        except redis.exceptions.ResponseError:
            return
        if value is not None:
            return self.serializer.loads(value)

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
                values[i] = self.serializer.loads(value)
        return values

    def _resolve_set(self, key, value,
                     replace=True,
                     unique=False) -> Union[List[Any], None]:
        """Resolve final key/value to be used based on key/value's
        existence and value's uniqueness."""
        if self._exists(key) and not replace:
            prev_value = self._get(key)
            if isinstance(prev_value, dict):
                prev_value = []
            elif unique and value in prev_value:
                return
            prev_value.append(value)
            value = prev_value
        else:
            value = [value]
        return self.serializer.dumps(value)

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
                return
            prev_value.append(value)
            value = prev_value
        else:
            value = [value]
        return self.serializer.dumps(value)

    def _set(self, key, value, **kwargs):
        value = self._resolve_set(key, value, **kwargs)
        if value is not None:
            self._dbp.set(key, value)

    def _mset(self, mapping, **kwargs):
        _mapping = {}
        for key, value in mapping.items():
            value = self._resolve_set(key, value, **kwargs)
            if value is not None:
                _mapping[key] = value
        if len(_mapping) > 0:
            self._dbp.mset(_mapping)

    def _hset(self, key, field, value, **kwargs):
        value = self._resolve_hset(key, field, value, **kwargs)
        if value is not None:
            try:
                self._dbp.hset(key, field, value)
            except redis.exceptions.ResponseError:
                self._delete([key])
                self._dbp.hset(key, field, value)

    def _hmset(self, key, mapping, **kwargs):
        _mapping = {}
        for field, value in mapping.items():
            value = self._resolve_hset(key, field, value, **kwargs)
            if value is not None:
                _mapping[field] = value
        if len(_mapping) > 0:
            self._dbp.hmset(key, _mapping)

    def _keys(self):
        return list(map(self.serializer.loads, self._db.keys()))

    def _hkeys(self, key):
        try:
            fields = self._db.hkeys(key)
        except redis.exceptions.ResponseError:
            return []
        return list(map(self.serializer.loads, fields))

    def _len(self):
        return self._db.dbsize()

    def _hlen(self, key):
        try:
            return self._db.hlen(key)
        except redis.exceptions.ResponseError:
            return 0

    def _exists(self, key):
        return bool(self._db.exists(key))

    def _hexists(self, key, field):
        try:
            return bool(self._db.hexists(key, field))
        except redis.exceptions.ResponseError:
            return False

    def _delete(self, keys):
        for key in keys:
            self._db.delete(key)

    def _hdelete(self, key, fields):
        for field in fields:
            try:
                self._db.hdel(key, field)
            except redis.exceptions.ResponseError:
                pass

    def execute(self):
        if self.is_pipe:
            self._dbp.execute()

    def close(self):
        """Redis object is disconnected automatically when object
        goes out of scope."""
        self.execute()
        self.save()

    def flush(self):
        self._db.flushdb()

    def save(self, **kwargs):
        self._db.save()

    def config(self, mapping={}, **kwargs):
        if len(mapping) == 0:
            return [self._db.config_get(), self._db.info()]
        else:
            for key, value in mapping.items():
                self._db.config_set(key, value)


# class RedisSearcher:
#     def __init__(self,
#                  feature_extractor,
#                  host=os.environ.get('REDIS_HOST', 'localhost'),
#                  port=os.environ.get('REDIS_PORT', 6379),
#                  database=os.environ.get('REDIS_DB', 0),
#                  **kwargs):
#         self.feature_extractor = feature_extractor
#         self.db = redis.Redis(host=host, port=port, db=database)
#         self._db = self.db.pipeline()
#         self.serializer = kwargs.get('serializer', Serializer())
#
#     def add(self, string):
#         features = self.feature_extractor.features(string)
#         if self.db.exists(len(features)):
#             # NOTE: Optimization idea is to remove duplicate features.
#             # Probably this should be handled by the feature extractor.
#             # For now, let us assume that features are unique.
#             # NOTE: Optimization idea is to use a cache for hkeys to
#             # prevent hitting database as much.
#             prev_features = self.db.hkeys(len(features))
#
#             # NOTE: Decode previous features, so that we only manage strings.
#             # Also, use set for fast membership test.
#             prev_features = set(map(bytes.decode, prev_features))
#
#             for feature in features:
#                 if feature in prev_features:
#                     strings = \
#                         self.lookup_strings_by_feature_set_size_and_feature(
#                             len(features),
#                             feature
#                         )
#                     if string not in strings:
#                         strings.add(string)
#                         self._db.hmset(
#                             len(features),
#                             {feature: self.serializer.serialize(strings)}
#                         )
#                 else:
#                     self._db.hmset(
#                         len(features),
#                         {feature: self.serializer.serialize(set((string,)))}
#                     )
#         else:
#             for feature in features:
#                 self._db.hmset(
#                     len(features),
#                     {feature: self.serializer.serialize(set((string,)))}
#                 )
#         self._db.execute()
#
#     def lookup_strings_by_feature_set_size_and_feature(self, size, feature):
#         # NOTE: Redis returns a list
#         res = self.db.hmget(size, feature)[0]
#         return self.serializer.deserialize(res) if res is not None else set()
#
#     def all(self):
#         strings = set()
#         for key in self.db.keys():
#             for values in map(self.serializer.deserialize,
#                               self.db.hvals(key)):
#                 strings.update(values)
#         return strings
#
#     def clear(self):
#         self.db.flushdb()
