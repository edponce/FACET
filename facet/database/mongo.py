import sys
import time
import pymongo
from .base import BaseDatabase
from ..helpers import (
    parse_address,
    expand_envvars,
)
from typing import (
    Any,
    Dict,
    Tuple,
    Union,
    Iterable,
)


__all__ = ['MongoDatabase']


class MongoDatabase(BaseDatabase):
    """Mongo database interface.

    Args:
        host (str): Host name of database connection.

        port (int): Port number of database connection.

        database (str): Database name.

        collection (str): Collection name.

        keys (str, Iterable[Tuple[str, Any]]): Keys for index collection.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit()' command to submit commands in pipe.

        connect (bool): If set, automatically connect during initialization.

        max_connect_attempts (int): Number of times to attempt connecting to
            database during object instantiation. There is no connection
            handling if connection disconnects at any other moment.

    Kwargs: Options forwarded to 'MongoClient' class.
    """

    NAME = 'mongo'

    def __init__(
        self,
        database: str = 'test',
        *,
        collection: str = 'strings',
        host: str = 'localhost',
        port: int = 27017,
        keys: Union[str, Iterable[Tuple[str, Any]]] = 'text',
        access_mode: str = 'c',
        use_pipeline: bool = False,
        connect: bool = True,
        max_connect_attempts: int = 1,
        **conn_info,
    ):
        self._conn = None
        self._db = None
        self._db_name = database
        self._collection = None
        self._collection_name = collection
        self._pipeline = None
        self._host = host
        self._port = port
        self._keys = keys
        self._access_mode = access_mode
        self._use_pipeline = use_pipeline
        self._max_connect_attempts = max_connect_attempts
        self._conn_info = conn_info

        if connect:
            self.connect()
        else:
            self._pre_connect()

    def _pre_connect(self, **kwargs):
        self._db_name = expand_envvars(kwargs.pop('database', self._db_name))
        self._collection_name = expand_envvars(
            kwargs.pop('collection', self._collection_name)
        )
        self._host, self._port = parse_address(
            expand_envvars(kwargs.pop('host', self._host)),
            kwargs.pop('port', self._port),
        )
        self._keys = kwargs.pop('keys', self._keys)
        self._access_mode = kwargs.pop('access_mode', self._access_mode)
        self._use_pipeline = kwargs.pop('use_pipeline', self._use_pipeline)
        self._max_connect_attempts = kwargs.pop(
            'max_connect_attempts',
            self._max_connect_attempts,
        )
        self._conn_info.update(kwargs)

    def _post_connect(self):
        # Database gets created when documents are inserted
        self._db = self._conn.get_database(
            self._db_name,
            read_preference=(
                pymongo.ReadPreference.NEAREST
                if self._access_mode == 'r'
                else None
            ),
        )

        if self._access_mode in ('c', 'n'):
            if self._access_mode == 'n':
                self.clear()

            if self._collection_name not in self._db.list_collection_names():
                self._collection = self._db.create_collection(
                    self._collection_name,
                )
                self._collection.create_index(keys=self._keys, background=True)
            else:
                self._collection = self._db.get_collection(
                    self._collection_name,
                )
        else:
            self._collection = self._db.get_collection(self._collection_name)

        if self._use_pipeline:
            self._pipeline = {}

    def __len__(self):
        return self._collection.count_documents({})

    def __contains__(self, query):
        return bool(self.get(query))

    def __iter__(self):
        return iter(self._collection.find())

    @property
    def backend(self):
        return (self._conn, self._db, self._collection)

    def configuration(self):
        is_connected = self.ping()
        return {
            'connected': is_connected,
            'host': self._host,
            'port': self._port,
            'database': self._db_name,
            'collection': self._collection_name,
            'access mode': self._access_mode,
            'pipelined': self._use_pipeline,
            'max connect attempts': self._max_connect_attempts,
            'nrows': len(self) if is_connected else -1,
        }

    def info(self, **kwargs):
        return self._conn.server_info(**kwargs)

    def index_info(self, **kwargs):
        return self._collection.index_information(**kwargs)

    def get(self, query: Dict[str, Any], *, key=None, **kwargs):
        """
        Args:
            key (Any): A hashable value that is unique for the document,
                that is used as a key for storing in pipeline dictionary.
        """
        if self._use_pipeline and key is not None and key in self._pipeline:
            return self._pipeline[key]
        return self._collection.find(query, **kwargs)

    def set(self, document: Dict[str, Any], *, key=None, **kwargs):
        """
        Args:
            key (Any): A hashable value that is unique for the document,
                that is used as a key for storing in pipeline dictionary.
        """
        if self._use_pipeline and key is not None:
            self._pipeline[key] = document
        else:
            self._collection.insert_one(document, **kwargs)

    def delete(self, document, **kwargs):
        self._collection.delete_one(document, **kwargs)

    def connect(self, **kwargs):
        if self.ping():
            return

        self._pre_connect(**kwargs)

        connect_attempts = 0
        while True:
            connect_attempts += 1
            self._conn = pymongo.MongoClient(
                host=self._host,
                port=self._port,
                **self._conn_info,
            )
            is_connected, ex = self.ping(with_exception=True)
            if is_connected:
                break
            if connect_attempts >= self._max_connect_attempts:
                raise ex
            print('Warning: failed connecting to MongoDB at '
                  f'{self._host:self._port}, reconnection attempt '
                  f'{connect_attempts} ...',
                  file=sys.stderr)
            time.sleep(1)

        self._post_connect()

    def commit(self, **kwargs):
        if not self.ping():
            return
        if self._use_pipeline and self._pipeline:
            self._collection.insert_many(
                self._pipeline.values(),
                ordered=False,
                bypass_document_validation=True,
                **kwargs,
            )
            # NOTE: MongoDB periodically triggers flushes to service
            # pending writes from storage layer to disk, and locks the
            # entire mongod instance to prevent additional writes until
            # lock is released.
            # https://api.mongodb.com/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient.fsync
            # self._conn.fsync(**{'async': True})
            self._pipeline = {}

    def disconnect(self):
        if self.ping():
            self._conn.close()
            self._collection = None
            self._db = None
            # NOTE: PyMongo does not seems to close the connection, so
            # we disconnect manually.
            self._conn = None
            self._pipeline = None

    def clear(self):
        self._db.drop_collection(self._collection_name)
        self._collection = None
        if self._use_pipeline:
            self._pipeline = {}

    def drop_database(self):
        self._conn.drop_database(self._db)
        if self._use_pipeline:
            self._pipeline = {}

    def ping(self, with_exception: bool = False):
        is_connected = False
        ex = None
        if self._conn is not None:
            try:
                self._conn.admin.command('ismaster')
                is_connected = True
            except pymongo.errors.ConnectionFailure as ex:
                pass
        return (is_connected, ex) if with_exception else is_connected
