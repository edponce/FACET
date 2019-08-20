import os
import pickle
import shelve
from .base import BaseDatabase


__all__ = ['DictDatabase']


class DictDatabase(BaseDatabase):
    """Python dictionary database interface.

    Args:

        db (str): Database directory and name. The path is created
            if it does not exists. Default is current working directory.
            The database name is used as prefix for database files.

        flag (str): Database open mode. Valid modes are 'r' = read,
            'w' = write, 'c' = read/write/create, and 'n' = new.
            Default is 'c'. Additional characters can be appended to the
            flag for more control. Valid values are 'f' = fast/no sync,
            's' = sync, and 'u' = no lock.

        pipe (bool): If set, queue 'set-related' commands to cached database.
            Run 'sync' command to submit commands in pipe.
            Default is False.

        kwargs (Dict[str, Any]): Option forwarding, see 'pickle'.
            E.g., 'protocol' and 'encoding'.

    Notes:
        * The underyling database is managed by a persistent dictionary
          and values are serialized by 'pickle'.

        * Keys/fields are treated as ordinary 'str'.
    """
    MODES = {
        'r': 'read',
        'w': 'write',
        'c': 'read/write',
        'n': 'overwrite, read/write',
    }

    CONTROLS = {
        'f': 'fast, no sync',
        's': 'sync',
        'u': 'no lock',
        '': '',
    }

    def __init__(self,
                 db, *,
                 flag='c',
                 pipe=False,
                 **kwargs):
        db_dir, db_name = os.path.split(db)
        if not db_name:
            raise ValueError('missing database filename, no basename')
        db_dir = os.path.abspath(db_dir) if db_dir else os.getcwd()
        os.makedirs(db_dir, exist_ok=True)
        self._dir = db_dir
        self._name = db_name
        self._flag = flag
        self._is_pipe = pipe

        # Connect to database
        self._db = shelve.open(
            os.path.join(self._dir, self._name),
            flag=self._flag,
            writeback=self._is_pipe,
            protocol=kwargs.get('protocol', pickle.HIGHEST_PROTOCOL),
            **kwargs,
        )

    @property
    def config(self):
        db_file = os.path.join(self._dir, self._name + '.dat')
        return {
            'name': self._name,
            'dir': self._dir,
            'mode': type(self).MODES[self._flag[0]],
            'control': type(self).CONTROLS[
                self._flag[1] if len(self._flag) > 1 else ''
            ],
            'pipe': self._is_pipe,
            'used_memory': (os.path.getsize(db_file)
                            if os.path.exists(db_file) else 0),
            'keys': len(self),
        }

    def _is_hash_name(self, key: str) -> bool:
        """Detect if a key is a hash name.
        Hash maps use a 'defaultdict' for representing fields/values.
        """
        return isinstance(self._db[key], dict) if self._exists(key) else False

    def _get(self, key):
        return self._db[key] if self._exists(key) else None

    def _mget(self, keys):
        return [self._get(key) for key in keys]

    def _hget(self, key, field):
        return self._db[key][field] if self._is_hash_name(key) else None

    def _hmget(self, key, fields):
        values = len(fields) * [None]
        if self._is_hash_name(key):
            mapping = self._db[key]
            for i, field in enumerate(fields):
                if field in mapping:
                    values[i] = mapping[field]
        return values

    def _set(self, key, value, **kwargs):
        value = self._resolve_set(key, value, **kwargs)
        if value is not None:
            self._db[key] = value

    def _mset(self, mapping, **kwargs):
        for key, value in mapping.items():
            value = self._resolve_set(key, value, **kwargs)
            if value is not None:
                self._db[key] = value

    def _hset(self, key, field, value, **kwargs):
        value = self._resolve_hset(key, field, value, **kwargs)
        if value is not None:
            self._db[key] = {field: value}

    def _hmset(self, key, mapping, **kwargs):
        _mapping = {}
        for field, value in mapping.items():
            value = self._resolve_hset(key, field, value, **kwargs)
            if value is not None:
                _mapping[field] = value
        if len(_mapping) > 0:
            self._db[key] = _mapping

    def _keys(self):
        return list(self._db.keys())

    def _hkeys(self, key):
        fields = []
        if self._exists(key):
            mapping = self._db[key]
            if isinstance(mapping, dict):
                fields = self._db[key].keys()
        return list(fields)

    def _len(self):
        return len(self._db)

    def _hlen(self, key):
        _len = 0
        if self._exists(key):
            mapping = self._db[key]
            if isinstance(mapping, dict):
                _len = len(mapping)
        return _len

    def _exists(self, key):
        return key in self._db

    def _hexists(self, key, field):
        valid = False
        if self._exists(key):
            mapping = self._db[key]
            if isinstance(mapping, dict) and field in mapping:
                valid = True
        return valid

    def _delete(self, keys):
        for key in keys:
            if self._exists(key):
                del self._db[key]

    def _hdelete(self, key, fields):
        for field in fields:
            if self._hexists(key, field):
                del self._db[key][field]

    def sync(self):
        if self._is_pipe:
            self._db.sync()

    def close(self):
        self._db.close()

    def clear(self):
        self._db.clear()

    save = sync
