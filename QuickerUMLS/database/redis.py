import redis
from .base import BaseDatabase


class RedisDatabase(BaseDatabase):
    """Redis database interface.

    Args:
        host (str): See Redis documentation. Default is 'localhost'.

        port (int): See Redis documentation. Default is 6379.

        db (int): Database ID, see Redis documentation. Default is 0.

        pipe (bool): If set, queue commands to Redis database.
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
        self._db = redis.Redis(host=self.host, port=self.port, db=self.db_id)
        if self._pipe:
            self._db = self.db.pipeline()
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

    @property
    def db(self):
        return self._db

    def get(self, key):
        value = self.db.get(key)
        if value is not None:
            value = self.serializer.loads(value)
        return value

    def set(self, key, value,
            extend=False, unique=False, **kwargs):
        if not extend or not self.exists(key):
            if not isinstance(value, (str, bytes)):
                value = self.serializer.dumps(value)
        else:
            prev_value = self.get(key)
            if not unique:
                if not isinstance(prev_value, list):
                    prev_value = [prev_value]
            else:
                if isinstance(prev_value, list):
                    if value not in prev_value:
                        value = self.serializer.dumps(prev_value.append(value))
                        self.db.set(key, value, **kwargs)
                else:
                    if value != prev_value:
                        prev_value = [prev_value]
            value = self.serializer.dumps(prev_value.append(value))
        self.db.set(key, value, **kwargs)

    def mget(self, keys):
        return list(map(
            self.serializer.loads,
            filter(lambda v: v is not None, self.db.mget(keys))
        ))

    def mset(self, mapping,
             extend=False, unique=False):
        for key, value in mapping.items():
            if not isinstance(value, (str, bytes)):
                mapping[key] = self.serializer.dumps(value)
        self.db.mset(mapping)

    def hget(self, key, field):
        value = self.db.hget(key, field)
        if value is not None:
            value = self.serializer.loads(value)
        return value

    def hset(self, key, field, value,
             extend=False, unique=False):
        if not isinstance(value, (str, bytes)):
            value = self.serializer.dumps(value)
        self.db.hset(key, field, value)

    def hmget(self, key, fields):
        return list(map(
            self.serializer.loads,
            filter(lambda v: v is not None, self.db.hmget(key, fields))
        ))

    def hmset(self, key, mapping,
              extend=False, unique=False):
        for field, value in mapping.items():
            if not isinstance(value, (str, bytes)):
                mapping[field] = self.serializer.dumps(value)
        self.db.hmset(key, mapping)

    def keys(self):
        return list(map(self.serializer.loads, self.db.keys()))

    def hkeys(self, key):
        return list(map(self.serializer.loads, self.db.hkeys(key)))

    def exists(self, key):
        return bool(self.db.exists(key))

    def hexists(self, key, field):
        return bool(self.db.hexists(key, field))

    def delete(self, keys):
        self.db.delete(keys)

    def hdelete(self, key, fields):
        self.db.hdel(key, fields)

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
