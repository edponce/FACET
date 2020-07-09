import copy
import redis
from .base import BaseKVDatabase
from ..serializer import (
    serializer_map,
    BaseSerializer,
)
from ..utils import (
    parse_address,
    get_obj_map_key,
)
from typing import Union


__all__ = ['RedisDatabase']


class RedisDatabase(BaseKVDatabase):
    """Redis database interface.

    Args:
        host (str): Host name of database connection.

        port (int): Port number of database connection.

        n (int): Database number, see Redis documentation.

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
        host: str = 'localhost',
        *,
        port: int = 6379,
        n: int = 0,
        access_mode: str = 'c',
        use_pipeline: bool = False,
        serializer: Union[str, 'BaseSerializer'] = 'json',
        **conn_info,
    ):
        self._conn = None
        self._conn_pipe = None
        self._host, self._port = parse_address(host, port)
        self._n = n
        self._use_pipeline = use_pipeline
        self._serializer = None
        self.serializer = serializer
        self._is_connected = False
        self._conn_info = copy.deepcopy(conn_info)

        self.connect()

        # Reset database based on access mode
        if access_mode == 'n':
            self.clear()

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
        return bool(self._conn.exists(key))

    def __len__(self):
        return self._conn.dbsize()

    def get_config(self):
        return {
            'host': self._host,
            'port': self._port,
            'db num': self._n,
            'item count': len(self) if self._is_connected else -1,
            'use pipeline': self._use_pipeline,
            'serializer': get_obj_map_key(self._serializer, serializer_map),
        }

    def get_info(self):
        if not self._is_connected:
            return {}
        info = copy.deepcopy(self._conn.info())
        info.update(self._conn.config_get())
        return info

    def get(self, key):
        value = self._conn.get(key)
        return value if value is None else self._serializer.loads(value)

    def set(self, key, value):
        self._conn_pipe.set(key, self._serializer.dumps(value))

    def keys(self):
        return list(map(lambda k: k.decode(), self._conn.keys()))

    def delete(self, key):
        self._conn.delete(key)

    def connect(self):
        self._conn = redis.Redis(
            host=self._host,
            port=self._port,
            db=self._n,
            **self._conn_info,
        )

        self._conn_pipe = (
            self._conn.pipeline()
            if self._use_pipeline
            else self._conn
        )
        self._is_connected = True

    def commit(self):
        if self._is_connected and self._use_pipeline:
            self._conn_pipe.execute()

    def disconnect(self):
        self._conn = None
        self._conn_pipe = None
        self._is_connected = False

    def clear(self):
        self._conn.flushdb()
