import sys
import copy
import time
import redis
import redisearch
from .base import (
    BaseDatabase,
    BaseKVDatabase,
)
from ..serializer import (
    serializer_map,
    BaseSerializer,
)
from ..helpers import (
    parse_address,
    get_obj_map_key,
)
from typing import (
    Any,
    Dict,
    Union,
    Iterable,
)


__all__ = [
    'RedisDatabase',
    'RediSearchDatabase',
]


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

    def __init__(
        self,
        n: int = 0,
        *,
        host: str = 'localhost',
        port: int = 6379,
        access_mode: str = 'c',
        use_pipeline: bool = False,
        max_connect_attempts: int = 3,
        serializer: Union[str, 'BaseSerializer'] = 'json',
        **conn_info,
    ):
        self._conn = None
        self._conn_pipe = None
        self._host, self._port = parse_address(host, port)
        self._n = n
        self._use_pipeline = use_pipeline
        self._max_connect_attempts = max_connect_attempts
        self._serializer = None
        self.serializer = serializer
        self._is_connected = False
        self._conn_info = copy.deepcopy(conn_info)

        self.connect()

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

    def __len__(self):
        return self._conn.dbsize()

    def __contains__(self, key):
        return bool(self._conn.exists(key))

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
        info = copy.deepcopy(self._conn.info())
        info.update(self._conn.config_get())
        return info

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
        return list(map(lambda k: k.decode(), self._conn.keys()))

    def delete(self, key):
        self._conn.delete(key)

    def connect(self):
        if self._is_connected:
            return
        connect_attempts = 0
        while True:
            connect_attempts += 1
            try:
                self._conn = redis.Redis(
                    host=self._host,
                    port=self._port,
                    db=self._n,
                    **self._conn_info,
                )
                break
            except redis.exceptions.ConnectionError as ex:
                if connect_attempts >= self._max_connect_attempts:
                    raise ex
                print('Warning: failed connecting to Redis database at '
                      f'{self._host:self._port}, reconnection attempt '
                      f'{connect_attempts} ...',
                      file=sys.stderr)
                time.sleep(1)
        if self._use_pipeline:
            self._conn_pipe = self._conn.pipeline()
            self._pipeline = {}
        else:
            self._conn_pipe = self._conn
        self._is_connected = True

    def commit(self):
        if self._is_connected and self._use_pipeline:
            if self._pipeline:
                self._pipeline = {}
            self._conn_pipe.execute()

    def disconnect(self):
        if self._is_connected:
            self._pipeline = None
            self._is_connected = False
            self._conn_pipe = None
            self._conn = None

    def clear(self):
        if self._use_pipeline:
            self._pipeline = {}
        self._conn.flushdb()


class RediSearchDatabase(BaseDatabase):
    """RediSearch database interface.

    Args:
        host (str): Host name of database connection.

        port (int): Port number of database connection.

        index (str): Index name.

        fields (Iterable[Any]): Index fields specified by RediSearch types.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit' command to submit commands in pipe.

        max_connect_attempts (int): Number of times to attempt connecting to
            database during object instantiation. There is no connection
            handling if connection disconnects at any other moment.

    Kwargs: Options forwarded to 'Redis' and 'redisearch.Client' classes.

    Notes:
        * (Recommended) Use a Docker RediSearch container
          https://hub.docker.com/r/redislabs/redisearch/

        * (Alternative) Install RediSearch module, see
          https://redislabs.com/blog/mastering-redisearch-part
    """
    def __init__(
        self,
        index: str = 'facet',
        *,
        fields: Iterable[Any],
        host: str = 'localhost',
        port: int = 6379,
        access_mode: str = 'c',
        use_pipeline: bool = False,
        max_connect_attempts: int = 3,
        **conn_info,
    ):
        self._conn = None
        self._conn_pipe = None
        self._host, self._port = parse_address(host, port)
        self._index = index
        self._use_pipeline = use_pipeline
        self._max_connect_attempts = max_connect_attempts
        self._is_connected = False
        self._conn_info = copy.deepcopy(conn_info)

        self.connect()

        if access_mode in ('c', 'n'):
            # NOTE: If index does not exist, an exception for 'Unknown
            # index name' is triggered.
            try:
                self._conn.info()
                index_exists = True
            except redis.exceptions.ResponseError:
                index_exists = False

            if index_exists and access_mode == 'n':
                self.clear()
                index_exists = False

            if not index_exists:
                self._conn.create_index(
                    fields,
                    # no_term_offsets=False,
                    # no_field_flags=False,
                    # stopwords=None,
                )

    def __len__(self):
        return int(self._conn.info()['num_docs'])

    def __contains__(self, id):
        return NotImplemented

    def __getitem__(self, id):
        return self._conn.load_document(id)

    def __setitem__(self, id, document):
        self.set(id, document)

    def __delitem__(self, id):
        self.delete(id)

    def get_config(self):
        return {
            'host': self._host,
            'port': self._port,
            'item count': len(self) if self._is_connected else -1,
            'use pipeline': self._use_pipeline,
        }

    def get_info(self):
        return self._conn.info()

    def get(self, query: Union[str, Any], **kwargs):
        return self._conn.search(query, **kwargs)

    def set(self, id, document: Dict[str, Any], **kwargs):
        self._conn_pipe.add_document(id, **kwargs, **document)

    def delete(self, id, **kwargs):
        self._conn.delete_document(id, **kwargs)

    def connect(self):
        connect_attempts = 0
        while True:
            connect_attempts += 1
            try:
                self._conn = redisearch.Client(
                    self._index,
                    host=self._host,
                    port=self._port,
                    **self._conn_info,
                )
                break
            except redis.exceptions.ConnectionError as ex:
                if connect_attempts >= self._max_connect_attempts:
                    raise ex
                print('Warning: failed connecting to RediSearch database at '
                      f'{self._host:self._port}, reconnection attempt '
                      f'{connect_attempts} ...',
                      file=sys.stderr)
                time.sleep(1)

        self._conn_pipe = (
            self._conn.batch_indexer(chunk_size=1000)
            if self._use_pipeline
            else self._conn
        )
        self._is_connected = True

    def commit(self):
        if self._is_connected and self._use_pipeline:
            self._conn_pipe.commit()

    def disconnect(self):
        if self._is_connected:
            self._conn = None
            self._conn_pipe = None
            self._is_connected = False

    def clear(self):
        self._conn.drop_index()
