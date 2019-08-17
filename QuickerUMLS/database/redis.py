import redis
from .base import BaseDatabase
from typing import Union, List, Any


class RedisDatabase(BaseDatabase):
    """Redis database interface.

    Args:
        host (str): See Redis documentation. Default is 'localhost'.

        port (int): See Redis documentation. Default is 6379.

        db (int): Database ID, see Redis documentation. Default is 0.

        pipe (bool): If set, queue 'set-related' commands to Redis database.
            Run 'execute' command to submit commands in pipe.
            Default is False.

    Note:
        * Redis accepts keys/fields objects as either str or bytes.
    """

    def __init__(self, **kwargs):
        self._host = kwargs.get('host', 'localhost')
        self._port = kwargs.get('port', 6379)
        self._db_id = kwargs.get('db', 0)
        self._pipe = kwargs.get('pipe', False)
        self._db = redis.Redis(
            host=self._host,
            port=self._port,
            db=self._db_id,
        )
        # NOTE: Redis pipeline object is used only for 'set' operations
        # and requires invoking 'execute' to commit queued operations.
        self._dbp = self._db.pipeline() if self._pipe else self._db
        super().__init__(**kwargs)

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
    def pipe(self):
        return self._pipe

    def _resolve_set(self, key, value,
                     extend=False,
                     unique=False) -> Union[List[Any], None]:
        if self.exists(key) and extend:
            prev_value = self.get(key)
            if unique and value in prev_value:
                return
            prev_value.append(value)
            value = prev_value
        else:
            value = [value]
        return self.serializer.dumps(value)

    def _resolve_hset(self, key, field, value,
                      extend=False,
                      unique=False) -> Union[List[Any], None]:
        if self.hexists(key, field) and extend:
            prev_value = self.hget(key, field)
            if unique and value in prev_value:
                return
            prev_value.append(value)
            value = prev_value
        else:
            value = [value]
        return self.serializer.dumps(value)

    def get(self, key):
        value = self._db.get(key)
        if value is not None:
            value = self.serializer.loads(value)
        return value

    def set(self, key, value, **kwargs):
        value = self._resolve_set(key, value, **kwargs)
        if value is not None:
            self._dbp.set(key, value)

    def mget(self, keys):
        return list(map(
            self.serializer.loads,
            filter(lambda v: v is not None, self._db.mget(keys))
        ))

    def mset(self, mapping, **kwargs):
        _mapping = {}
        for key, value in mapping.items():
            value = self._resolve_set(key, value, **kwargs)
            if value is not None:
                _mapping[key] = value
        if len(_mapping) > 0:
            self._dbp.mset(_mapping)

    def hget(self, key, field):
        value = self._db.hget(key, field)
        if value is not None:
            value = self.serializer.loads(value)
        return value

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
        return list(map(self.serializer.loads, self._db.keys()))

    def hkeys(self, key):
        return list(map(self.serializer.loads, self._db.hkeys(key)))

    def exists(self, key):
        return bool(self._db.exists(key))

    def hexists(self, key, field):
        return bool(self._db.hexists(key, field))

    def delete(self, keys):
        for key in keys:
            self._db.delete(key)

    def hdelete(self, key, fields):
        for field in fields:
            self._db.hdel(key, field)

    def close(self):
        """Redis object is disconnected automatically when object
        goes out of scope."""
        self.execute()

    def flush(self):
        self._db.flushdb()

    def execute(self):
        if self.pipe:
            self._dbp.execute()


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
#             prev_features = self.db.hkeys(len(features))
#
#             # NOTE: Decode previous features, so that we only manage strings.
#             # Also, use set for fast membership test.
#             prev_features = set(map(bytes.decode, prev_features))
#
#             for feature in features:
#                 if feature in prev_features:
#                     strings = self.lookup_strings_by_feature_set_size_and_feature(len(features), feature)
#                     if string not in strings:
#                         strings.add(string)
#                         self._db.hmset(len(features), {feature: self.serializer.serialize(strings)})
#                 else:
#                     self._db.hmset(len(features), {feature: self.serializer.serialize(set((string,)))})
#         else:
#             for feature in features:
#                 self._db.hmset(len(features), {feature: self.serializer.serialize(set((string,)))})
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
#             for values in map(self.serializer.deserialize, self.db.hvals(key)):
#                 strings.update(values)
#         return strings
#
#     def clear(self):
#         self.db.flushdb()
