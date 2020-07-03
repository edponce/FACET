import os
import sqlite3
from .base import BaseDatabase
from ..serializer import (
    serializer_map,
    BaseSerializer,
)
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Iterable,
    Iterator,
    NoReturn,
)


__all__ = ['SQLiteDatabase']


class SQLiteDatabase(BaseDatabase):
    """SQLite database interface.

    Args:
        table (str): Name of database table.

        db (str): Path representing database directory and name for persistent
            database. The path is created if it does not exists. The
            database name is used as prefix for database files. If None or
            empty string, an in-memory database is used. Default is None.

        flag (str): (For persistent mode only) Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists.

        pipe (bool): If set, queue 'set-related' commands to database.
            Run 'sync' command to submit commands in pipe.
            Default is False.

        serializer (str, BaseSerializer): Serializer instance or serializer
            name. Valid serializers are: 'json', 'yaml', 'pickle', 'string',
            'stringsj'. Default is 'json'.

    Kwargs (see 'sqlite3.connect()'):
        timeout
        detect_types
        isolation_level
        check_same_thread
        cached_statements
    """

    def __init__(
        self,
        *,
        table: str,
        db: str = None,
        table_type: str = 'kv',
        flag: str = 'c',
        pipe=False,
        serializer: Union[str, 'BaseSerializer'] = 'json',
        **kwargs,
    ):
        if db:
            # Persistent database
            db_dir, db_name = os.path.split(db)
            if not db_name:
                raise ValueError('missing database filename, no basename')
            db_dir = os.path.abspath(db_dir) if db_dir else os.getcwd()
            os.makedirs(db_dir, exist_ok=True)
            db_name = db

            options = '?'
            if flag == 'r':
                options += 'mode=ro'
                options += '&nolock=1'
            elif flag == 'w':
                options += 'mode=rw'
            elif flag == 'c':
                options += 'mode=rwc'
            else:
                options = ''

            db_uri = 'file:' + db + options
            persistent = True

        else:
            # In-memory database
            db_name = ':memory:'
            db_uri = db_name
            persistent = False

        self._name = db_name
        self._table = table
        self._type = table_type
        self._persistent = persistent
        self._serializer = None
        self.serializer = serializer

        # Connect to database
        # Types supported: bytes, str, int, float, None
        self._db = sqlite3.connect(db_uri, uri=True, **kwargs)

        # Create table
        if self._type == 'kv':
            self._db.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table} ("
                "  key VARCHAR PRIMARY KEY,"
                "  value BLOB NOT NULL"
                ");"
            )
        elif self._type == 'simstring':
            pass

    @property
    def config(self):
        return {
            'name': self._name,
            'table': self._table,
            'type': self._type,
        }

    @property
    def serializer(self):
        return self._serializer

    @serializer.setter
    def serializer(self, value: Union[str, 'BaseSerializer']):
        if isinstance(value, str):
            obj = serializer_map[value]()
        elif isinstance(value, BaseSerializer):
            obj = value
        else:
            raise ValueError(f'invalid serializer, {value}')
        self._serializer = obj

    def __getitem__(
        self,
        key: Union[str, Iterable[str]],
    ) -> Union[List[Any], None, List[Union[List[Any], None]]]:
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> NoReturn:
        self.set(key, value)

    def __delitem__(self, key: str) -> NoReturn:
        self.delete(key)

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        cur = self._db.execute(f"SELECT COUNT(key) FROM {self._table}")
        return cur.fetchone()[0]

    def items(self) -> Iterator[Tuple[str, Any]]:
        yield self._db.execute(f"SELECT key, value FROM {self._table}")

    # def _resolve_set(
    #     self,
    #     key,
    #     value,
    #     *,
    #     replace=None,
    #     unique=False,
    # ) -> Union[List[Any], None]:
    #     """Resolve final key/value to be used based on key/value's
    #     existence and value's uniqueness."""
    #     if replace is not None:
    #         if not replace and self.exists(key):
    #             prev_value = self.get(key)
    #             if isinstance(prev_value, dict):
    #                 prev_value = []
    #             elif unique and value in prev_value:
    #                 return None
    #             prev_value.append(value)
    #             value = prev_value
    #         else:
    #             value = [value]
    #     return value

    def get(self, key):
        cur = self._db.execute(
            f"SELECT value FROM {self._table} "
            f"WHERE key='{key}'"
        )

        value = cur.fetchone()
        return self._serializer.loads(value[0]) if value is not None else value

    def mget(self, keys):
        cur = self._db.execute(
            f"SELECT value FROM {self._table} "
            "WHERE key in ({})".format(', '.join('?' for _ in keys)), keys
        )
        return list(map(lambda x: x[0], cur.fetchall()))

    def set(self, key, value, **kwargs):
        cur = self._db.execute(
            f"INSERT INTO {self._table}(key, value) "
            "VALUES (?, ?)", (key, self._serializer.dumps(value))
        )

    def mset(self, mapping, **kwargs):
        pass
    # def mset(self, mapping, **kwargs):
    #     _mapping = {}
    #     for key, value in mapping.items():
    #         value = self._resolve_set(key, value, **kwargs)
    #         if value is not None:
    #             _mapping[key] = self._serializer.dumps(value)
    #     if len(_mapping) > 0:
    #         self._dbp.mset(_mapping)

    def keys(self):
        cur = self._db.execute(f"SELECT key FROM {self._table}")
        return map(lambda row: row[0], cur)

    def exists(self, key):
        return self.get(key) is not None

    def delete(self, key):
        self._db.execute(
            f"DELETE FROM {self._table} "
            f"WHERE key='{key}'"
        )

    def sync(self):
        if self._db.in_transaction:
            self._db.commit()

    save = sync

    def close(self):
        self.sync()
        self._db.close()

    def clear(self):
        # NOTE: Delete table vs. delete all rows. The former triggers error
        # during 'Facet._install()'.
        # self._db.execute(f"DROP TABLE IF EXISTS {self._table}")
        self._db.execute(f"DELETE FROM {self._table}")

    def set_pipe(self, pipe: bool) -> NoReturn:
        """Enable/disable pipeline mode."""
        pass

    # ABSTRACT METHODS (Delete)
    _get = get
    _mget = mget

    def _hget(self, key: str, field: str) -> Union[List[Any], None]:
        pass

    def _hmget(
        self,
        key: str,
        fields: Iterable[str]
    ) -> List[Union[List[Any], None]]:
        pass

    _set = set
    _mset = mset

    def _hset(
        self, key: str, field: str, value: Any, *,
        replace=None, unique=False,
    ) -> NoReturn:
        pass

    def _hmset(
        self, key: str, mapping: Dict[str, Any], *,
        replace=None, unique=False,
    ) -> NoReturn:
        pass

    _keys = keys

    def _hkeys(self, key: str) -> List[str]:
        pass

    _len = __len__

    def _hlen(self, key: str) -> int:
        """Return number of fields in hash map.
        If key is not a hash name, then return 0
        If key does not exists, then return 0.
        """
        pass

    _exists = exists

    def _hexists(self, key: str, field: str) -> bool:
        """Check existence of a hash name.
        If key is not a hash name, then return false.
        """
        pass

    _delete = delete

    def _hdelete(self, key: str, fields: Iterable[str]) -> NoReturn:
        pass
