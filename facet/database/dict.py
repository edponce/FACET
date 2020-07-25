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
    """Persistent dictionary database using 'shelve' module.

    Args:
        filename (str): Path representing database directory and name for
            persistent dictionary. The underlying database is managed as
            a file-backed dictionary. A '.dat' extension is automatically
            added by Shelf to the name. The database name is used as
            prefix for database files. Depending on 'access_mode', the
            path may be created.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit' command to submit commands in pipe.

        connect (bool): If set, automatically connect during initialization.

        protocol (int): Pickle serialization protocol to use.

    Notes:
        * Read-only access mode does not necessarily prevents writes.
          Is OS/system-dependent behavior in Shelf?

        * Shelf automatically invokes 'sync()' when closing database.

        * Writeback allows natural operations on mutable entries,
          but consumes more memory and makes sync/close operations take
          longer. Writeback queues operations to a database cache, but
          because Python language does not allows detecting when a mutation
          occurs, read operations are also cached. Use 'sync()' command to
          empty the cache and synchronize with disk. As a consequence to
          how writeback works, 'get()' operations are successful immediately
          after 'set()' operations.
    """

    NAME = 'dict'

    def __init__(
        self,
        filename: str,
        *,
        access_mode: str = 'c',
        use_pipeline: bool = False,
        protocol=pickle.HIGHEST_PROTOCOL,
        connect: bool = True,
    ):
        self._conn = None
        self._filename = filename
        self._name = None
        self._access_mode = access_mode
        self._use_pipeline = use_pipeline
        self._protocol = protocol

        if connect:
            self.connect()
        else:
            self._pre_connect(make_dir=False)

    def _pre_connect(self, make_dir: bool = True, **kwargs):
        """Resolves database name and filenames, and creates directory."""
        self._filename = expand_envvars(kwargs.pop('filename', self._filename))
        self._access_mode = kwargs.pop('access_mode', self._access_mode)
        self._use_pipeline = kwargs.pop('use_pipeline', self._use_pipeline)
        self._protocol = kwargs.pop('protocol', self._protocol)

        db_dir, self._name = os.path.split(self._filename)
        if make_dir and db_dir and self._access_mode in ('c', 'n'):
            os.makedirs(db_dir, exist_ok=True)

        # NOTE: Shelve automatically appends '.dat' extension to file name.
        # Also, it creates additional files with '.dir' and '.bak' extensions.
        self._files = tuple(
            '{}.{}'.format(self._filename, ext)
            for ext in ('dat', 'dir', 'bak')
        )

    def __len__(self):
        return len(self._conn)

    def __contains__(self, key):
        return key in self._conn

    @property
    def backend(self):
        return self._conn

    def configuration(self):
        is_connected = self.ping()
        return {
            'connected': is_connected,
            'name': self._name,
            'files': self._files,
            'access mode': self._access_mode,
            'pipelined': self._use_pipeline,
            'serialization protocol': self._protocol,
            'nrows': len(self._conn) if is_connected else -1,
            'store': os.path.getsize(self._files[0]) if is_connected else -1,
        }

    def get(self, key):
        return self._conn.get(key)

    def set(self, key, value):
        self._conn[key] = value

    def keys(self):
        return iter(self._conn.keys())

    def delete(self, key):
        del self._conn[key]

    def connect(self, **kwargs):
        if self.ping():
            return

        self._pre_connect()

        self._conn = shelve.open(
            self._filename,
            writeback=self._use_pipeline,
            flag=self._access_mode,
            protocol=self._protocol,
        )

    def commit(self):
        if self.ping() and self._use_pipeline:
            self._conn.sync()

    def disconnect(self):
        if self.ping():
            self._conn.close()

    def clear(self):
        # NOTE: 'clear()' removes items but file size remains the same.
        self._conn.clear()

    def ping(self):
        # NOTE: Shelf's backend dictionary gets a 'closed()' method
        # when closed.
        return (
            hasattr(self._conn, 'dict')
            and not hasattr(self._conn.dict, 'closed')
        )


class MemoryDictDatabase(BaseKVDatabase):
    """In-memory key/value database.

    Args:
        connect (bool): If set, automatically connect during initialization.
    """

    NAME = 'dict'

    def __init__(self, connect: bool = True):
        self._conn = None
        if connect:
            self.connect()

    def __len__(self):
        return len(self._conn)

    def __contains__(self, key):
        return key in self._conn

    @property
    def backend(self):
        return self._conn

    def configuration(self):
        is_connected = self.ping()
        return {
            'connected': is_connected,
            'nrows': len(self._conn) if is_connected else -1,
            'store': sys.getsizeof(self._conn) if is_connected else -1,
        }

    def get(self, key):
        return self._conn.get(key)

    def set(self, key, value):
        self._conn[key] = value

    def keys(self):
        return self._conn.keys()

    def delete(self, key):
        del self._conn[key]

    def connect(self):
        if not self.ping():
            self._conn = {}

    def disconnect(self):
        if self.ping():
            self._conn = None

    def clear(self):
        self._conn = {}

    def ping(self):
        return self._conn is not None


class DictDatabase:
    """Factory class for dictionary-based database."""

    NAME = 'dict'

    def __call__(self, filename=None, **kwargs):
        if filename:
            kwargs['filename'] = filename
            cls = FileDictDatabase
        else:
            cls = MemoryDictDatabase
        return cls(**kwargs)
