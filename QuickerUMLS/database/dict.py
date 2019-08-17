import collections
from .base import BaseDatabase
from typing import Union, List, Any


class DictDatabase(BaseDatabase):
    """Python dictionary database interface."""

    def __init__(self, **kwargs):
        self._db = collections.defaultdict(list)
        super().__init__(**kwargs)

    @property
    def db(self):
        return self._db

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
        value = self.db.get(key)
        if value is not None:
            value = self.serializer.loads(value)
        return value

    def set(self, key, value, **kwargs):
        value = self._resolve_set(key, value, **kwargs)
        if value is not None:
            self.db.set(key, value)

    def mget(self, keys):
        return list(map(
            self.serializer.loads,
            filter(lambda v: v is not None, self.db.mget(keys))
        ))

    def mset(self, mapping, **kwargs):
        _mapping = {}
        for key, value in mapping.items():
            value = self._resolve_set(key, value, **kwargs)
            if value is not None:
                _mapping[key] = value
        if len(_mapping) > 0:
            self.db.mset(_mapping)

    def hget(self, key, field):
        value = self.db.hget(key, field)
        if value is not None:
            value = self.serializer.loads(value)
        return value

    def hset(self, key, field, value, **kwargs):
        value = self._resolve_hset(key, value, **kwargs)
        if value is not None:
            self.db.hset(key, field, value)

    def hmget(self, key, fields):
        return list(map(
            self.serializer.loads,
            filter(lambda v: v is not None, self.db.hmget(key, fields))
        ))

    def hmset(self, key, mapping, **kwargs):
        _mapping = {}
        for field, value in mapping.items():
            value = self._resolve_hset(key, field, value, **kwargs)
            if value is not None:
                _mapping[field] = value
        if len(_mapping) > 0:
            self.db.hmset(key, _mapping)

    def keys(self):
        return list(map(self.serializer.loads, self.db.keys()))

    def hkeys(self, key):
        return list(map(self.serializer.loads, self.db.hkeys(key)))

    def exists(self, key):
        return bool(self.db.exists(key))

    def hexists(self, key, field):
        return bool(self.db.hexists(key, field))

    def delete(self, keys):
        for key in keys:
            self.db.delete(key)

    def hdelete(self, key, fields):
        for field in fields:
            self.db.hdel(key, field)

    def close(self):
        """Redis object is disconnected automatically when object
        goes out of scope."""
        self.execute()

    def flush(self):
        self.db.flushdb()

    def execute(self):
        if self.pipe:
            self.db.execute()


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
