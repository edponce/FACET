import copy
import pymongo
from .base import BaseDatabase
from ..helpers import parse_address
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
)


__all__ = ['MongoDatabase']


class MongoDatabase(BaseDatabase):
    """Mongo database interface.

    Args:
        host (str): Host name of database connection.

        port (int): Port number of database connection.

        database (str): Database name.

        index (str, List[Tuple[str, Any]]): Index or indices for collection.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit()' command to submit commands in pipe.
            Default is False.

    Kwargs:
        Options forwarded to 'MongoClient' class.
    """

    def __init__(
        self,
        database: str,
        *,
        host: str = 'localhost',
        port: int = 27017,
        index: Union[str, List[Tuple[str, Any]]] = None,
        access_mode: str = 'c',
        use_pipeline: bool = False,
        **conn_info,
    ):
        self._conn = None
        self._db = None
        self._database = database
        self._collection = None
        self._index = index
        self._dbp = []
        self._host, self._port = parse_address(host, port)
        self._use_pipeline = use_pipeline
        self._is_connected = False
        self._conn_info = copy.deepcopy(conn_info)

        self.connect()

        # Reset database based on access mode
        if access_mode == 'n':
            self.clear()

        if access_mode in ('c', 'n') and index:
            self._collection.create_index(keys=index, background=True)

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
            'database': self._database,
            'item count': len(self) if self._is_connected else -1,
            'index info': (
                self._collection.index_information()
                if self._is_connected
                else {}
            ),
        }

    def get_info(self, **kwargs):
        return self._conn.server_info(**kwargs)

    def get(self, query: Dict[str, Any], **kwargs):
        return self._collection.find(query, **kwargs)

    def set(self, document: Dict[str, Any], **kwargs):
        if self._use_pipeline:
            self._dbp.append(document)
        else:
            self._collection.insert_one(document, **kwargs)

    def delete(self, document, **kwargs):
        self._collection.delete_one(document, **kwargs)

    def connect(self):
        self._conn = pymongo.MongoClient(
            host=self._host,
            port=self._port,
            **self._conn_info,
        )

        # Database gets created when documents are inserted
        self._db = self._conn.get_database(self._database)
        self._collection = self._db.strings

        self._is_connected = True

    def commit(self, **kwargs):
        if self._is_connected and self._use_pipeline:
            if self._dbp:
                self._collection.insert_many(
                    self._dbp,
                    ordered=False,
                    bypass_document_validation=True,
                    **kwargs,
                )
                self._dbp = []
            # self._conn.fsync(**kwargs)

    def disconnect(self):
        if self._is_connected:
            self._conn.close()
            self._dbp = []
            self._is_connected = False

    def clear(self, **kwargs):
        self._collection.drop(**kwargs)
        self._dbp = []

    def drop_database(self):
        self._conn.drop_database(self._db)
        self._dbp = []
