import copy
import redis
from .base import BaseKVDatabase
from ..serializer import (
    serializer_map,
    BaseSerializer,
)
from ..utils import parse_address
from typing import Union


__all__ = [
    'RedisKVDatabase',
    'RedisDatabase',
]


class RedisKVDatabase(BaseKVDatabase):
    """Redis database interface.

    Args:
        host (str): See Redis documentation.

        port (int): See Redis documentation.

        id (int): Database ID, see Redis documentation.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit' command to submit commands in pipe.

        serializer (str, BaseSerializer): Serializer instance or serializer
            name.

    Kwargs:
        Options forwarded to 'Redis' class.

    Notes:
        * Redis treats keys/fields of 'str, bytes, and int'
          types interchangeably, but this interface accepts keys/fields
          of 'str' type and arbitrary values.

        * Redis Python API returns keys as 'bytes', so we use str.decode.

        * Redis object is disconnected automatically when object goes out
          of scope.
    """

    def __init__(
        self,
        host='localhost',
        *,
        port=6379,
        id=0,
        access_mode='c',
        use_pipeline: bool = False,
        serializer: Union[str, 'BaseSerializer'] = 'json',
        **kwargs,
    ):
        self._db = None
        self._dbp = None
        self._host, self._port = parse_address(host, port)
        self._uri = host
        self._db_id = id
        self._access_mode = None
        self._use_pipeline = use_pipeline
        self._serializer = None
        self.serializer = serializer
        self._is_connected = False
        self._conn_params = copy.deepcopy(kwargs)

        self.connect(access_mode=access_mode)

    @property
    def serializer(self):
        return self._serializer

    @serializer.setter
    def serializer(self, value: Union[str, 'BaseSerializer']):
        if value is None or isinstance(value, str):
            obj = serializer_map[value]()
        elif isinstance(value, BaseSerializer):
            obj = value
        else:
            raise ValueError(f'invalid serializer, {value}')
        self._serializer = obj

    def __contains__(self, key):
        return bool(self._db.exists(key))

    def __len__(self):
        return self._db.dbsize()

    def get_config(self):
        return {
            'host': self._host,
            'port': self._port,
            'db id': self._db_id,
            'uri': self._uri,
            'access mode': self._access_mode,
            'item count': len(self) if self._is_connected else -1,
            'use pipeline': self._use_pipeline,
            'serializer': self._serializer,
        }

    def get_info(self):
        info = copy.deepcopy(self._db.info())
        info.update(self._db.config_get())
        return info if self._is_connected else {}

    def get(self, key):
        value = self._db.get(key)
        return value if value is None else self._serializer.loads(value)

    def set(self, key, value):
        self._dbp.set(key, self._serializer.dumps(value))

    def keys(self):
        return list(map(lambda k: k.decode(), self._db.keys()))

    def delete(self, key):
        self._db.delete(key)

    def connect(self, *, access_mode='w'):
        if self._is_connected:
            if self._access_mode == access_mode:
                return
            self.disconnect()

        self._db = redis.Redis(
            host=self._host,
            port=self._port,
            db=self._db_id,
            **self._conn_params,
        )

        self._dbp = self._db.pipeline() if self._use_pipeline else self._db
        self._access_mode = access_mode
        self._is_connected = True

    def commit(self):
        if self._is_connected and self._use_pipeline:
            self._dbp.execute()

    def disconnect(self):
        self._db = None
        self._dbp = None
        self._is_connected = False

    def clear(self):
        self._db.flushdb()


def RedisDatabase(*args, db_type='kv', **kwargs):
    if db_type == 'kv':
        cls = RedisKVDatabase
    else:
        raise ValueError(f'invalid database type, {db_type}')
    return cls(*args, **kwargs)
