import sys
import time
import redis
from .base import BaseKVDatabase
from ..serializer import (
    get_serializer,
    BaseSerializer,
)
from ..helpers import (
    parse_address,
    expand_envvars,
)
from typing import Union


__all__ = ['RedisDatabase']


class RedisDatabase(BaseKVDatabase):
    """Redis database interface.

    Args:
        host (str): Host name of database connection.

        port (int): Port number of database connection.

        n (int): Database index, see Redis documentation.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit' command to submit commands in pipe.

        connect (bool): If set, automatically connect during initialization.

        max_connect_attempts (int): Number of times to attempt connecting to
            database during object instantiation. There is no connection
            handling if connection disconnects at any other moment.

        serializer (str, BaseSerializer): Serializer instance or serializer
            name.

    Kwargs: Options forwarded to 'redis.Redis' class.

    Notes:
        * Redis treats keys/fields of 'str, bytes, and int'
          types interchangeably, but this interface accepts keys/fields
          of 'str' type and arbitrary values.

        * Redis Python API returns keys as 'bytes', so we use str.decode.

        * Redis object is disconnected automatically when object goes out
          of scope.

        * In pipeline mode, asides from 'set', only 'get' checks the
          pipeline for uncommitted data.
    """

    NAME = 'redis'

    def __init__(
        self,
        n: int = 0,
        *,
        host: str = 'localhost',
        port: int = 6379,
        access_mode: str = 'c',
        use_pipeline: bool = False,
        connect: bool = True,
        max_connect_attempts: int = 1,
        serializer: Union[str, 'BaseSerializer'] = 'pickle',
        **conn_info,
    ):
        self._conn = None
        self._conn_pipe = None
        self._pipeline = None
        self._host = host
        self._port = port
        self._n = n
        self._access_mode = access_mode
        self._use_pipeline = use_pipeline
        self._max_connect_attempts = max_connect_attempts
        self._serializer = serializer
        self._conn_info = conn_info

        if connect:
            self.connect()
        else:
            self._pre_connect()

    def _pre_connect(self, **kwargs):
        self._host, self._port = parse_address(
            expand_envvars(kwargs.pop('host', self._host)),
            kwargs.pop('port', self._port),
        )
        self._n = kwargs.pop('n', self._n)
        self._access_mode = kwargs.pop('access_mode', self._access_mode)
        self._use_pipeline = kwargs.pop('use_pipeline', self._use_pipeline)
        self._max_connect_attempts = kwargs.pop(
            'max_connect_attempts',
            self._max_connect_attempts,
        )
        self._serializer = get_serializer(
            kwargs.pop('serializer', self._serializer)
        )
        self._conn_info.update(kwargs)

    def _post_connect(self):
        if self._access_mode == 'n':
            self.clear()

        if self._use_pipeline:
            self._conn_pipe = self._conn.pipeline()
            self._pipeline = {}
        else:
            self._conn_pipe = self._conn

    def __len__(self):
        return self._conn.dbsize()

    def __contains__(self, key):
        return bool(self._conn.exists(key))

    @property
    def backend(self):
        return self._conn

    def configuration(self):
        is_connected = self.ping()
        return {
            'connected': is_connected,
            'host': self._host,
            'port': self._port,
            'n': self._n,
            'access mode': self._access_mode,
            'pipelined': self._use_pipeline,
            'max connect attempts': self._max_connect_attempts,
            'serializer': type(self._serializer).NAME,
            'store': self._conn.info()['used_memory'] if is_connected else -1,
            'nrows': len(self) if is_connected else -1,
        }

    def info(self):
        return self._conn.info()

    def get(self, key):
        if self._use_pipeline and key in self._pipeline:
            return self._pipeline[key]
        value = self._conn.get(key)
        return value if value is None else self._serializer.loads(value)

    def set(self, key, value):
        if self._use_pipeline:
            self._pipeline[key] = value
        self._conn_pipe.set(key, self._serializer.dumps(value))

    def keys(self):
        return list(map(lambda x: x.decode(), self._conn.keys()))

    def delete(self, key):
        self._conn.delete(key)

    def connect(self, **kwargs):
        if self.ping():
            return

        self._pre_connect(**kwargs)

        ex = ConnectionError
        for connect_attempt in range(1, self._max_connect_attempts + 1):
            try:
                self._conn = redis.Redis(
                    host=self._host,
                    port=self._port,
                    db=self._n,
                    **self._conn_info,
                )
                if self.ping():
                    break
            except redis.exceptions.ConnectionError as exc:
                # NOTE: This variable set is to prevent linter errors because
                # it does not recognizes 'name' in 'exception ... as name'.
                ex = exc
                print('Warning: failed connecting to Redis database at '
                      f'{self._host:self._port}, reconnection attempt '
                      f'{connect_attempt} ...',
                      file=sys.stderr)
                time.sleep(1)
        else:
            raise ex(
                'failed connecting to Redis database at '
                f'{self._host:self._port}.'
            )

        self._post_connect()

    def commit(self):
        if not self.ping():
            return
        if self._use_pipeline and self._pipeline:
            self._conn_pipe.execute()
            self._pipeline = {}

    def disconnect(self):
        if self.ping():
            self._conn_pipe = None
            self._conn = None
            self._pipeline = None

    def clear(self):
        self._conn.flushdb()
        if self._use_pipeline:
            self._pipeline = {}

    def ping(self):
        return self._conn is not None and self._conn.ping()
