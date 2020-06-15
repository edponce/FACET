import os
import sys
import pickle
import shelve
from .base import BaseDatabase


__all__ = ['DictDatabase']


class DictDatabase(BaseDatabase):
    """Python dictionary database interface
    Supports both persistent and in-memory dictionaries.

    Args:

        db (str): Path representing database directory and name for persistent
            dictionary. The underlying database is managed by a file-backed
            dictionary. A '.dat' extension is automatically added by 'shelve'
            to the filename. The path is created if it does not exists. The
            database name is used as prefix for database files. If None or
            empty string, an in-memory dictionary is used. Default is None.

        flag (str): (For persistent mode only) Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write',
            'c' = read/write/create if not exists, 'n' = new read/write.
            Default is 'c'.

        protocol (int): (For persistent mode only) Pickle serialization
            protocol to use. Default is 'pickle.HIGHEST_PROTOCOL'.

        pipe (bool): (For persistent mode only) If set, queue 'set-related'
            commands to database. Run 'sync' command to submit commands
            in pipe. Default is True.
    """

    def __init__(
        self,
        db: str = None,
        *,
        flag: str = 'c',
        protocol: int = pickle.HIGHEST_PROTOCOL,
        pipe: bool = True,
    ):
        if db:
            # Persistent dictionary
            db_dir, db_name = os.path.split(db)
            if not db_name:
                raise ValueError('missing database filename, no basename')
            db_dir = os.path.abspath(db_dir) if db_dir else os.getcwd()
            os.makedirs(db_dir, exist_ok=True)
            persistent = True
            # NOTE: Enable writeback because it allows natural operations
            # on mutable entries, but consumes more memory and makes
            # sync/close operations take longer. Writeback queues operations
            # to database cache. Use sync command to empty the cache and
            # synchronize with disk. Sync is automatically called when
            # the database is closed.
            self._db = shelve.open(
                os.path.join(db_dir, db_name),
                writeback=pipe,
                flag=flag,
                protocol=protocol,
            )
        else:
            # In-memory dictionary
            db_dir = None
            db_name = None
            persistent = False
            pipe = False
            self._db = {}

        self._dir = db_dir
        self._name = db_name
        self._persistent = persistent
        self._is_pipe = pipe

    def set_pipe(self, pipe):
        pass

    @property
    def config(self):
        if self._persistent:
            # NOTE: shelve automatically adds '.dat' extension to file.
            db_file = os.path.join(self._dir, self._name + '.dat')
        else:
            db_file = None

        return {
            'name': self._name,
            'dir': self._dir,
            'used_memory': (os.path.getsize(db_file)
                            if self._persistent and os.path.exists(db_file)
                            else sys.getsizeof(self._db)),
            'keys': len(self),
        }

    def _is_hash_name(self, key: str) -> bool:
        """Detect if a key is a hash name.
        Hash maps use a dictionary for representing fields/values.
        """
        return self._exists(key) and isinstance(self._db[key], dict)

    def _get(self, key):
        return self._db.get(key)

    def _mget(self, keys):
        return [self._get(key) for key in keys]

    def _hget(self, key, field):
        return (
            self._db[key].get(field)
            if self._is_hash_name(key)
            else None
        )

    def _hmget(self, key, fields):
        return (
            [self._db[key].get(field) for field in fields]
            if self._is_hash_name(key)
            else len(fields) * [None]
        )

    def _set(self, key, value, **kwargs):
        value = self._resolve_set(key, value, **kwargs)
        if value is not None:
            self._db[key] = value

    def _mset(self, mapping, **kwargs):
        for key, value in mapping.items():
            self._set(key, value, **kwargs)

    def _hset(self, key, field, value, **kwargs):
        value = self._resolve_hset(key, field, value, **kwargs)
        if value is not None:
            if key in self._db:
                self._db[key].update({field: value})
            else:
                self._db[key] = {field: value}

    def _hmset(self, key, mapping, **kwargs):
        for field, value in mapping.items():
            self._hset(key, field, value, **kwargs)

    def _keys(self):
        return list(self._db.keys())

    def _hkeys(self, key):
        return list(self._db[key].keys()) if self._is_hash_name(key) else []

    def _len(self):
        return len(self._db)

    def _hlen(self, key):
        return len(self._db[key]) if self._is_hash_name(key) else 0

    def _exists(self, key):
        return key in self._db

    def _hexists(self, key, field):
        return self._is_hash_name(key) and field in self._db[key]

    def _delete(self, keys):
        for i, key in enumerate(filter(self._exists, keys), start=1):
            del self._db[key]

    def _hdelete(self, key, fields):
        if self._is_hash_name(key):
            for i, field in enumerate(filter(lambda f: f in self._db[key],
                                             fields),
                                      start=1):
                del self._db[key][field]
            # NOTE: Delete key if it has no fields remaining
            if len(self._db[key]) == 0:
                del self._db[key]

    def sync(self):
        if self._pipe:
            self._db.sync()

    def close(self):
        if self._persistent:
            self._db.close()

    def clear(self):
        if self._persistent:
            self._db.clear()
        else:
            self._db = {}

    save = sync
