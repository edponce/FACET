import os
import sys
import shelve
import pickle
from .base2 import BaseKVDatabase


__all__ = [
    'FileDict_KVDatabase',
    'MemoryDict_KVDatabase',
    'DictKVDatabase',
    'DictDatabase',
]


def parse_filename(filename):
    fdir, fname = os.path.split(filename)
    fdir = os.path.abspath(fdir) if fdir else os.getcwd()
    return fdir, fname


class FileDict_KVDatabase(BaseKVDatabase):
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

        protocol (int): Pickle serialization protocol to use.

    Notes:
        * Shelf automatically syncs when closing database.
    """

    def __init__(
        self,
        filename,
        *,
        access_mode='c',
        protocol=pickle.HIGHEST_PROTOCOL,
    ):
        self._db = None
        self._name = None
        self._file = None
        self._access_mode = None
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
        return str(key) in self._db

    def _check_write_access(self):
        if self._access_mode == 'r':
            raise ValueError('invalid operation on read-only shelf')

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
        return self._db[str(key)]

    def set(self, key, value):
        self._check_write_access()
        self._db[str(key)] = value

    def keys(self):
        return iter(self._db.keys())

    def delete(self, key):
        self._check_write_access()
        del self._db[str(key)]

    def connect(self, *, access_mode='w'):
        if self._is_connected:
            if self._access_mode == access_mode:
                return
            self._db.close()

        # NOTE: Writeback allows natural operations on mutable entries,
        # but consumes more memory and makes sync/close operations take
        # longer. Writeback queues operations to a database cache. But
        # because Python language does not allow detecting when a mutation
        # occurs, read operations are also cached. Use sync command to
        # empty the cache and synchronize with disk.
        self._db = shelve.open(
            self._name,
            # writeback=not self._access_mode == 'r',
            writeback=False,
            flag=access_mode,
            protocol=self._protocol,
        )

        self._access_mode = access_mode
        self._is_connected = True

    def disconnect(self):
        if self._is_connected:
            self._db.close()
            self._is_connected = False

    def commit(self):
        if self._access_mode != 'r':
            self._db.sync()

    def clear(self):
        self._check_write_access()
        self._db.clear()


class MemoryDict_KVDatabase(BaseKVDatabase):
    """In-memory dictionary database.

    Args:
        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.
    """

    def __init__(self, *, access_mode='c'):
        self._db = None
        self._access_mode = None
        self._is_connected = False
        self.connect(access_mode=access_mode)

    def __len__(self):
        self._check_connection_access()
        return len(self._db)

    def __contains__(self, key):
        self._check_connection_access()
        return key in self._db

    def _check_connection_access(self):
        if not self._is_connected:
            raise ValueError('invalid operation on closed database')

    def _check_write_access(self):
        if self._access_mode == 'r':
            raise ValueError('invalid operation on read-only database')

    def get_config(self):
        return {
            'access mode': self._access_mode,
            'memory usage': sys.getsizeof(self._db),
            'item count': len(self._db) if self._is_connected else -1,
        }

    def get(self, key):
        self._check_connection_access()
        return self._db[key]

    def set(self, key, value):
        self._check_connection_access()
        self._check_write_access()
        self._db[key] = value

    def keys(self):
        self._check_connection_access()
        return self._db.keys()

    def delete(self, key):
        self._check_connection_access()
        self._check_write_access()
        del self._db[key]

    def connect(self, *, access_mode='w'):
        if (
            (access_mode == 'c' and self._db is None)
            or access_mode == 'n'
        ):
            self._db = {}
        self._access_mode = access_mode
        self._is_connected = True

    def disconnect(self):
        self._is_connected = False

    def clear(self):
        self._check_connection_access()
        self._check_write_access()
        self._db = {}


def DictDatabase(*args, db_type='kv', **kwargs):
    """Factory function for dictionary-based database.

    Args:
        db_type (str): Type of database. Valid values are: 'kv', 'il'.
    """
    if db_type == 'kv':
        # NOTE: Using the number of arguments for identifying database is
        # not ideal (not extensible).
        if len(args) == 0:
            cls = MemoryDict_KVDatabase
        else:
            cls FileDict_KVDatabase
    elif db_type == 'il':
        pass
    else:
        raise ValueError(f'invalid database type, {db_type}')
    return cls(*args, **kwargs)
