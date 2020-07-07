import os
import sys
import shelve
import pickle
from .base import BaseKVDatabase
from ..utils import parse_filename


__all__ = [
    'FileDictKVDatabase',
    'MemoryDictKVDatabase',
    'DictDatabase',
]


class FileDictKVDatabase(BaseKVDatabase):
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
    """

    def __init__(
        self,
        filename,
        *,
        access_mode='c',
        use_pipeline: bool = False,
        protocol=pickle.HIGHEST_PROTOCOL,
    ):
        self._db = None
        self._name = None
        self._file = None
        self._access_mode = None
        self._use_pipeline = use_pipeline
        self._protocol = protocol
        self._is_connected = False

        db_dir, db_base = parse_filename(filename)
        if access_mode in ('c', 'n'):
            os.makedirs(db_dir, exist_ok=True)
        self._name = filename
        self._file = os.path.join(db_dir, db_base) + '.dat'

        self.connect(access_mode=access_mode)

    def __len__(self):
        return len(self._db)

    def __contains__(self, key):
        return key in self._db

    def get_config(self):
        return {
            'name': self._name,
            'file': self._file,
            'access mode': self._access_mode,
            'memory usage': os.path.getsize(self._file),
            'item count': len(self._db) if self._is_connected else -1,
            'serialization protocol': self._protocol,
        }

    def get(self, key):
        return self._db.get(key)

    def set(self, key, value):
        self._db[key] = value

    def keys(self):
        return iter(self._db.keys())

    def delete(self, key):
        del self._db[key]

    def connect(self, *, access_mode='w'):
        if self._is_connected:
            if self._access_mode == access_mode:
                return
            self.disconnect()

        # NOTE: Writeback allows natural operations on mutable entries,
        # but consumes more memory and makes sync/close operations take
        # longer. Writeback queues operations to a database cache. But
        # because Python language does not allow detecting when a mutation
        # occurs, read operations are also cached. Use sync command to
        # empty the cache and synchronize with disk.
        self._db = shelve.open(
            self._name,
            writeback=self._use_pipeline,
            flag=access_mode,
            protocol=self._protocol,
        )

        self._access_mode = access_mode
        self._is_connected = True

    def commit(self):
        self._db.sync()

    def disconnect(self):
        self._db.close()
        self._is_connected = False

    def clear(self):
        self._db.clear()


class MemoryDictKVDatabase(BaseKVDatabase):
    """In-memory key/value database."""

    def __init__(self):
        self._db = {}

    def __len__(self):
        return len(self._db)

    def __contains__(self, key):
        return key in self._db

    def get_config(self):
        return {
            'memory usage': sys.getsizeof(self._db),
            'item count': len(self._db),
        }

    def get(self, key):
        return self._db.get(key)

    def set(self, key, value):
        self._db[key] = value

    def keys(self):
        return self._db.keys()

    def delete(self, key):
        del self._db[key]

    def clear(self):
        self._db = {}


def DictDatabase(*args, filename=None, db_type='kv', **kwargs):
    """Factory function for dictionary-based database.

    Args:
        db_type (str): Type of database. Valid values are: 'kv'.
    """
    if db_type == 'kv':
        if filename:
            cls = FileDictKVDatabase
            kwargs['filename'] = filename
        else:
            cls = MemoryDictKVDatabase
    else:
        raise ValueError(f'invalid database type, {db_type}')
    return cls(*args, **kwargs)
