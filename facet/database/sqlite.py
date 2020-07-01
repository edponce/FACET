import os
import sqlite3
# from .base import BaseDatabase
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


class SQLiteDatabase:
    """SQLite database interface.

    Args:
        db (str): Path representing database directory and name for persistent
            database. The path is created if it does not exists. The
            database name is used as prefix for database files. If None or
            empty string, an in-memory database is used. Default is None.

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
        cached_statements
    """

    def __init__(
        self,
        *,
        table: str,
        db: str = None,
        table_type: str = 'kv',
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
            master_table = 'sqlite_master'
            persistent = True
        else:
            # In-memory database
            db_name = ':memory:'
            master_table = 'sqlite_temp_master'
            persistent = False

        self._name = db_name
        self._table = table
        self._type = table_type
        self._persistent = persistent
        self._serializer = None
        self.serializer = serializer

        # Connect to database
        # Types supported: bytes, str, int, float, None
        self._db = sqlite3.connect(self._name, **kwargs)

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

    # def mget(self, keys):
    #     cur = self._db.executemany(
    #         f"SELECT value FROM {self._table} "
    #         "WHERE key=(?)", keys
    #     )
    #     return cur.fetchall()

    def set(self, key, value, **kwargs):
        cur = self._db.execute(
            f"INSERT INTO {self._table}(key, value) "
            "VALUES (?, ?)", (key, self._serializer.dumps(value))
        )

    # def _mset(self, mapping, **kwargs):
    #     _mapping = {}
    #     for key, value in mapping.items():
    #         value = self._resolve_set(key, value, **kwargs)
    #         if value is not None:
    #             _mapping[key] = self._serializer.dumps(value)
    #     if len(_mapping) > 0:
    #         self._dbp.mset(_mapping)
    #
    # def _hset(self, key, field, value, **kwargs):
    #     value = self._resolve_hset(key, field, value, **kwargs)
    #     if value is not None:
    #         value = self._serializer.dumps(value)
    #         try:
    #             self._dbp.hset(key, field, value)
    #         except redis.exceptions.ResponseError:
    #             # NOTE: Assume key is not a hash name.
    #             self._delete([key])
    #             self._dbp.hset(key, field, value)
    #
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
        self._db.execute(f"DROP TABLE IF EXISTS {self._table}")

    def set_pipe(self, pipe: bool) -> NoReturn:
        """Enable/disable pipeline mode."""
        pass
