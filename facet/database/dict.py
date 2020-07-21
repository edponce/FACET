import os
import sys
import shelve
import pickle
from .base import BaseKVDatabase
from ..helpers import expand_envvars


__all__ = [
    'FileDictDatabase',
    'MemoryDictDatabase',
    'DictDatabase',
]


class FileDictDatabase(BaseKVDatabase):
    """Persistent dictionary database.

    Args:
        filename (str): Path representing database directory and name for
            persistent dictionary. The underlying database is managed as
            a file-backed dictionary. A '.dat' extension is automatically
            added by 'shelve' to the name. Depending on 'access_mode', the
            path may be created. The database name is used as prefix for
            database files.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit' command to submit commands in pipe.

        protocol (int): Pickle serialization protocol to use.

    Notes:
        * Shelf automatically syncs when closing database.

        * As a consequence to how writeback works, 'get' operations are
          supported immediately after 'set' operations.
    """

    def __init__(
        self,
        filename: str,
        *,
        access_mode: str = 'c',
        use_pipeline: bool = False,
        protocol=pickle.HIGHEST_PROTOCOL,
    ):
        self._conn = None
        self._name = None
        self._file = None
        self._access_mode = access_mode
        self._use_pipeline = use_pipeline
        self._protocol = protocol
        self._is_connected = False

        db_dir, db_base = os.path.split(expand_envvars(filename))
        if db_dir and access_mode in ('c', 'n'):
            os.makedirs(db_dir, exist_ok=True)
        self._name = filename
        self._file = os.path.join(db_dir, db_base) + '.dat'

        self.connect()

    def __len__(self):
        return len(self._conn)

    def __contains__(self, key):
        return key in self._conn

    def get_config(self):
        return {
            'name': self._name,
            'file': self._file,
            'access mode': self._access_mode,
            'memory usage': os.path.getsize(self._file),
            'item count': len(self._conn) if self._is_connected else -1,
            'serialization protocol': self._protocol,
        }

    def get(self, key):
        return self._conn.get(key)

    def set(self, key, value):
        self._conn[key] = value

    def keys(self):
        return iter(self._conn.keys())

    def delete(self, key):
        del self._conn[key]

    def connect(self):
        if self._is_connected:
            return
        # NOTE: Writeback allows natural operations on mutable entries,
        # but consumes more memory and makes sync/close operations take
        # longer. Writeback queues operations to a database cache. But
        # because Python language does not allow detecting when a mutation
        # occurs, read operations are also cached. Use sync command to
        # empty the cache and synchronize with disk.
        self._conn = shelve.open(
            self._name,
            writeback=self._use_pipeline,
            flag=self._access_mode,
            protocol=self._protocol,
        )
        self._is_connected = True

    def commit(self):
        if self._is_connected and self._use_pipeline:
            self._conn.sync()

    def disconnect(self):
        if self._is_connected:
            self._is_connected = False
            self._conn.close()

    def clear(self):
        self._conn.clear()


class MemoryDictDatabase(BaseKVDatabase):
    """In-memory key/value database."""

    def __init__(self):
        self._conn = None
        self._is_connected = False

        self.connect()

    def __len__(self):
        return len(self._conn)

    def __contains__(self, key):
        return key in self._conn

    def get_config(self):
        if self._is_connected:
            return {
                'memory usage': sys.getsizeof(self._conn),
                'item count': len(self._conn),
            }
        else:
            return {}

    def get(self, key):
        return self._conn.get(key)

    def set(self, key, value):
        self._conn[key] = value

    def keys(self):
        return self._conn.keys()

    def delete(self, key):
        del self._conn[key]

    def connect(self):
        if self._is_connected:
            return
        self._conn = {}
        self._is_connected = True

    def disconnect(self):
        if self._is_connected:
            self._is_connected = False
            self._conn = None

    def clear(self):
        self._conn = {}


def DictDatabase(filename=None, **kwargs):
    """Factory function for dictionary-based database."""
    if filename:
        kwargs['filename'] = filename
        cls = FileDictDatabase
    else:
        cls = MemoryDictDatabase
    return cls(**kwargs)
