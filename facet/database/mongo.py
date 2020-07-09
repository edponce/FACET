import os
import sys
import pymongo
from .base import BaseDatabase
from ..utils import parse_filename
from typing import (
    Any,
    Dict,
)


__all__ = ['MongoDatabase']


class MongoDatabase(BaseDatabase):
    """Mongo database interface.

    Args:
        host (str): Host name of database connection.

        port (int): Port number of database connection.

        database (str): Database name.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

    Kwargs:
        Options forwarded to 'MongoClient' class.
    """

    def __init__(
        self,
        host: str = 'localhost',
        *,
        port: int = 6379,
        database: str = 'simstring',
        access_mode: str = 'c',
        **conn_info,
    ):
        self._conn = None
        self._db = None
        self._dbname = database,
        self._access_mode = access_mode
        self._collection = None
        self._host, self._port = parse_address(host, port)
        self._is_connected = False
        self._conn_info = copy.deepcopy(conn_info)

        self.connect()

        # Reset database based on access mode
        if self._access_mode == 'n':
            self.clear()

        self._db = self._conn.get_database(self._dbname)
        self._collection = self._db.strings

    def __len__(self):
        return len(self._collection.find())

    def __contains__(self, query):
        return bool(self.get(query))

    def __iter__(self):
        return iter(self._collection.find())

    def get_config(self):
        return {
            'host': self._host,
            'port': self._port,
            'name': self._dbname,
            'access mode': self._access_mode,
            'item count': len(self._conn) if self._is_connected else -1,
        }

    def get_info(self):
        if not self._is_connected:
            return {}
        return self._conn.server_info()

    def get(self, query: Dict[str, Any], *, key: str = None):
        cur = self._collection.find(query)
        if key is None:
            data = set(cur)
        else:
            data = set(map(lambda x: x[key], cur))
        return data

    def set(self, data: Dict[str, Any]):
        self._collection.insert_one(data)

    def create_index(self, index):
        self._collection.create_index(index)

    # def delete(self, key):
    #     del self._conn[key]

    def connect(self):
        self._conn = pymongo.MongoClient(
            self._host,
            self._port,
        )
        self._is_connected = True

    def commit(self):
        self._conn.fsync()

    def disconnect(self):
        self._conn.close()
        self._is_connected = False

    def clear(self):
        self._collection.remove()

    def drop_database(self):
        self._conn.drop_database(self._db)
