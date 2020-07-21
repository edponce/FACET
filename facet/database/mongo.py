import copy
import pymongo
from .base import BaseDatabase
from ..helpers import parse_address
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

        max_connect_attempts (int): Number of times to attempt connecting to
            database during object instantiation. There is no connection
            handling if connection disconnects at any other moment.

    Kwargs: Options forwarded to 'MongoClient' class.
    """

    def __init__(
        self,
        database: str = 'facet',
        *,
        collection: str = 'strings',
        host: str = 'localhost',
        port: int = 27017,
        keys: Union[str, Iterable[Tuple[str, Any]]],
        access_mode: str = 'c',
        use_pipeline: bool = False,
        max_connect_attempts: int = 2,
        **conn_info,
    ):
        self._conn = None
        self._db = None
        self._db_name = database
        self._collection = None
        self._collection_name = collection
        self._pipeline = None
        self._host, self._port = parse_address(host, port)
        self._use_pipeline = use_pipeline
        self._max_connect_attempts = max_connect_attempts
        self._is_connected = False
        self._conn_info = copy.deepcopy(conn_info)

        self.connect()

        # Database gets created when documents are inserted
        self._db = self._conn.get_database(
            self._db_name,
            read_preference=(
                pymongo.ReadPreference.NEAREST
                if access_mode == 'r'
                else None
            ),
        )

        if access_mode == 'n':
            self.clear()

        if access_mode in ('c', 'n'):
            if self._collection_name not in self._db.list_collection_names():
                self._collection = self._db.create_collection(
                    self._collection_name,
                )
                self._collection.create_index(keys=keys, background=True)
            else:
                self._collection = self._db.get_collection(
                    self._collection_name,
                )
        else:
            self._collection = self._db.get_collection(self._collection_name)

    def __len__(self):
        return self._collection.count_documents({})

    def __contains__(self, query):
        return bool(self.get(query))

    def __iter__(self):
        return iter(self._collection.find())

    def get_config(self):
        return {
            'host': self._host,
            'port': self._port,
            'database': self._db_name,
            'collection': self._collection_name,
            'item count': len(self) if self._is_connected else -1,
            'index info': (
                self._collection.index_information()
                if self._is_connected
                else {}
            ),
        }

    def get_info(self, **kwargs):
        return self._conn.server_info(**kwargs)

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

    def connect(self):
        if self._is_connected:
            return
        connect_attempts = 0
        while True:
            connect_attempts += 1
            self._conn = pymongo.MongoClient(
                host=self._host,
                port=self._port,
                **self._conn_info,
            )
            try:
                self._conn.admin.command('ismaster')
                break
            except pymongo.errors.ConnectionFailure as ex:
                if connect_attempts >= self._max_connect_attempts:
                    raise ex
                print('Warning: failed connecting to Mongo database at '
                      f'{self._host:self._port}, reconnection attempt '
                      f'{connect_attempts} ...',
                      file=sys.stderr)
                time.sleep(1)
        if self._use_pipeline:
            self._pipeline = {}
        self._is_connected = True

    def commit(self, **kwargs):
        if self._is_connected and self._use_pipeline:
            if self._pipeline:
                self._collection.insert_many(
                    self._pipeline.values(),
                    ordered=False,
                    bypass_document_validation=True,
                    **kwargs,
                )
                self._pipeline = {}
            # NOTE: MongoDB periodically triggers flushes to service
            # pending writes from storage layer to disk, and locks the
            # entire mongod instance to prevent additional writes until
            # lock is released.
            # https://api.mongodb.com/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient.fsync
            # self._conn.fsync(**{'async': True})

    def disconnect(self):
        if self._is_connected:
            self._pipeline = None
            self._collection = None
            self._db = None
            self._is_connected = False
            self._conn.close()

    def clear(self):
        if self._use_pipeline:
            self._pipeline = {}
        if self._collection_name in self._db.list_collection_names():
            self._db.drop_collection(self._collection_name)
        self._collection = None

    def drop_database(self):
        if self._use_pipeline:
            self._pipeline = {}
        if self._db_name in self._conn.list_database_names():
            self._conn.drop_database(self._db)
        self._collection = None
        self._db = None
