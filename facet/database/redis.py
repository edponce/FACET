import sys
import time
import redis
import redisearch
from .base import (
    BaseDatabase,
    BaseKVDatabase,
)
from ..serializer import (
    get_serializer,
    BaseSerializer,
)
from ..helpers import (
    parse_address,
    expand_envvars,
)
from typing import (
    Any,
    List,
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

        connect (bool): If set, automatically connect during initialization.

        max_connect_attempts (int): Number of times to attempt connecting to
            database during object instantiation. There is no connection
            handling if connection disconnects at any other moment.

    Kwargs: Options forwarded to 'redis.Redis' classes.

    Notes:
        * (Recommended) Use a Docker RediSearch container
          https://hub.docker.com/r/redislabs/redisearch/

        * (Alternative) Install RediSearch module, see
          https://redislabs.com/blog/mastering-redisearch-part
    """

    NAME = 'redisearch'

    def __init__(
        self,
        index: str = 'test',
        *,
        fields: Iterable[Any] = (redisearch.TextField('text'),),
        host: str = 'localhost',
        port: int = 6379,
        access_mode: str = 'c',
        use_pipeline: bool = False,
        chunk_size: int = 10000,
        connect: bool = True,
        max_connect_attempts: int = 3,
        **conn_info,
    ):
        self._conn = None
        self._conn_pipe = None
        self._host = host
        self._port = port
        # NOTE: RediSearch index can only be created in database index 0.
        self._n = 0
        self._index = index
        self._fields = fields
        self._access_mode = access_mode
        self._use_pipeline = use_pipeline
        self._chunk_size = chunk_size
        self._max_connect_attempts = max_connect_attempts
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
        self._index = expand_envvars(kwargs.pop('index', self._index))
        self._access_mode = kwargs.pop('access_mode', self._access_mode)
        self._use_pipeline = kwargs.pop('use_pipeline', self._use_pipeline)
        self._chunk_size = kwargs.pop('chunk_size', self._chunk_size)
        self._max_connect_attempts = kwargs.pop(
            'max_connect_attempts',
            self._max_connect_attempts,
        )
        self._conn_info.update(kwargs)

    def _post_connect(self):
        if self._access_mode in ('c', 'n'):
            # NOTE: If index does not exist, an exception for 'Unknown
            # index name' is triggered.
            try:
                self._conn.info()
                index_exists = True
            except redis.exceptions.ResponseError:
                index_exists = False

            if index_exists and self._access_mode == 'n':
                # NOTE: 'clear()' creates index.
                self.clear()
                index_exists = True

            if not index_exists:
                self._conn.create_index(self._fields)

        # NOTE: 'clear()' creates pipeline connection, so we should not
        # do this again.
        self._conn_pipe = (
            self._conn.batch_indexer(chunk_size=self._chunk_size)
            if self._use_pipeline
            else self._conn
        )

    def __len__(self):
        return int(self._conn.info()['num_docs'])

    def __contains__(self, id):
        return bool(self[id])

    def __getitem__(self, id):
        # NOTE: RediSearch always returns a document, empty documents
        # only have 'id' and 'payload' fields.
        document = self._conn.load_document(id)
        return document if len(document.__dict__) > 2 else None

    def __setitem__(self, id, document):
        self.set(id, document)

    def __delitem__(self, id):
        self.delete(id)

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
            'index': self._index,
            'access mode': self._access_mode,
            'pipelined': self._use_pipeline,
            'max connect attempts': self._max_connect_attempts,
            'nrows': len(self) if is_connected else -1,
        }

    def info(self):
        return self._conn.info()

    def get(self, query: Union[str, Any], **kwargs):
        return self._conn.search(query, **kwargs)

    def set(self, id, document: Dict[str, Any], **kwargs):
        self._conn_pipe.add_document(id, **kwargs, **document)

    def delete(self, id, **kwargs):
        self._conn.delete_document(id, **kwargs)

    def connect(self, **kwargs):
        if self.ping():
            return

        self._pre_connect(**kwargs)

        ex = ConnectionError
        for connect_attempt in range(1, self._max_connect_attempts + 1):
            try:
                conn = redis.Redis(
                    host=self._host,
                    port=self._port,
                    db=self._n,
                    **self._conn_info,
                )
                self._conn = redisearch.Client(self._index, conn=conn)
                if self.ping():
                    break
            except redis.exceptions.ConnectionError as exc:
                ex = exc
                print('Warning: failed connecting to RediSearch database at '
                      f'{self._host:self._port}, reconnection attempt '
                      f'{connect_attempt} ...',
                      file=sys.stderr)
                time.sleep(1)
        else:
            raise ex(
                'failed connecting to RediSearch database at '
                f'{self._host:self._port}.'
            )

        self._post_connect()

    def commit(self):
        if self.ping() and self._use_pipeline:
            self._conn_pipe.commit()

    def disconnect(self):
        if self.ping():
            if self._use_pipeline:
                self._conn_pipe.pipeline.close()
            self._conn.redis.close()
            self._conn_pipe = None
            self._conn = None

    def clear(self):
        # NOTE: RediSearch does not supports deleting index content,
        # so we delete the database index and recreate the index.
        self._conn.redis.flushdb()
        self._conn.create_index(self._fields)
        self._conn_pipe = (
            self._conn.batch_indexer(chunk_size=self._chunk_size)
            if self._use_pipeline
            else self._conn
        )

    def drop_index(self):
        self._conn.drop_index()

    def ping(self):
        return self._conn is not None and self._conn.redis.ping()


class RediSearchAutoCompleterDatabase(BaseDatabase):
    """RediSearch AutoCompleter database interface.

    Args:
        host (str): Host name of database connection.

        port (int): Port number of database connection.

        n (int): Database index, see Redis documentation.

        key (str): Key for AutoCompleter data.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit' command to submit commands in pipe.

        connect (bool): If set, automatically connect during initialization.

        max_connect_attempts (int): Number of times to attempt connecting to
            database during object instantiation. There is no connection
            handling if connection disconnects at any other moment.

    Kwargs: Options forwarded to 'redis.Redis' class.
    """

    NAME = 'redisearch-autocompleter'

    def __init__(
        self,
        n: int = 0,
        key: str = 'ac',
        *,
        host: str = 'localhost',
        port: int = 6379,
        access_mode: str = 'c',
        use_pipeline: bool = False,
        connect: bool = True,
        max_connect_attempts: int = 3,
        **conn_info,
    ):
        self._conn = None
        self._pipeline = None
        self._host = host
        self._port = port
        self._n = n
        self._key = key
        self._access_mode = access_mode
        self._use_pipeline = use_pipeline
        self._max_connect_attempts = max_connect_attempts
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
        self._key = expand_envvars(kwargs.pop('key', self._key))
        self._access_mode = kwargs.pop('access_mode', self._access_mode)
        self._use_pipeline = kwargs.pop('use_pipeline', self._use_pipeline)
        self._max_connect_attempts = kwargs.pop(
            'max_connect_attempts',
            self._max_connect_attempts,
        )
        self._conn_info.update(kwargs)

    def _post_connect(self):
        if self._access_mode == 'n':
            self.clear()
        elif self._use_pipeline:
            self._pipeline = []

    def __len__(self):
        return self._conn.len()

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
            'key': self._key,
            'access mode': self._access_mode,
            'pipelined': self._use_pipeline,
            'max connect attempts': self._max_connect_attempts,
            'nrows': len(self) if is_connected else -1,
        }

    def get(self, query: str, **kwargs) -> List[redisearch.Suggestion]:
        return self._conn.get_suggestions(query, **kwargs)

    def set(self, suggestion: Union[str, redisearch.Suggestion], **kwargs):
        if not isinstance(suggestion, redisearch.Suggestion):
            suggestion = redisearch.Suggestion(suggestion, 0.5)
        if self._use_pipeline:
            self._pipeline.append(suggestion)
        else:
            self._conn.add_suggestions(suggestion, increment=True, **kwargs)

    def delete(self, string):
        self._conn.delete(string)

    def connect(self, **kwargs):
        if self.ping():
            return

        self._pre_connect(**kwargs)

        ex = ConnectionError
        for connect_attempt in range(1, self._max_connect_attempts + 1):
            try:
                conn = redis.Redis(
                    host=self._host,
                    port=self._port,
                    db=self._n,
                    **self._conn_info,
                )
                self._conn = redisearch.AutoCompleter(self._key, conn=conn)
                if self.ping():
                    break
            except redis.exceptions.ConnectionError as exc:
                ex = exc
                print('Warning: failed connecting to RediSearch database at '
                      f'{self._host:self._port}, reconnection attempt '
                      f'{connect_attempt} ...',
                      file=sys.stderr)
                time.sleep(1)
        else:
            raise ex(
                'failed connecting to RediSearch database at '
                f'{self._host:self._port}.'
            )

        self._post_connect()

    def commit(self, **kwargs):
        if self.ping() and self._use_pipeline:
            self._conn.add_suggestions(
                *self._pipeline,
                increment=True,
                **kwargs,
            )
            self._use_pipeline = []

    def disconnect(self):
        if self.ping():
            self._conn.redis.close()
            self._conn = None
            self._pipeline = None

    def clear(self):
        self._conn.redis.flushdb()
        if self._use_pipeline:
            self._pipeline = []

    def ping(self):
        return self._conn is not None and self._conn.redis.ping()
