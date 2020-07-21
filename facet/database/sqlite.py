import os
import copy
import sqlite3
import urllib.parse
from .base import BaseKVDatabase
from ..helpers import (
    parse_address_query,
    unparse_address_query,
    expand_envvars,
    get_obj_map_key,
)
from ..serializer import (
    serializer_map,
    BaseSerializer,
)
from typing import (
    Any,
    Tuple,
    Union,
    Iterator,
    Iterable,
)


__all__ = ['SQLiteDatabase']


class SQLiteDatabase(BaseKVDatabase):
    """SQLite database interface.

    Args:
        filename (str): Path representing database directory and name for
            persistent database. The path is created if it does not exists.
            The database name is used as prefix for database files. If None
            or empty string, a memory database is used.
            For unnamed in-memory database, set filename as ':memory:'.
            For named in-memory database, use URI format and set 'mode=memory'
            in query part. Example: 'file:memdb?mode=memory&cache=shared'

        table (str): Name of database table.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit' command to submit commands in pipe.

        key_type (type): Python-based type for keys in database.
            Valid values are: 'str', 'int', 'bool', 'float', 'bytes'.
            Internally, sqlite3 converts keys to the selected type.

        value_type (type): Python-based type for values in database.
            Valid values are: 'str', 'int', 'bool', 'float', 'bytes', None.
            If None, then any type is allowed and the serializer is used for
            converting values to binary form. Use 'bytes' when you are already
            providing binary data.

        serializer (str, BaseSerializer): Serializer instance or serializer
            name.

    Kwargs: Options forwarded to 'sqlite3.connect()'. For example,
        timeout, detect_types, isolation_level, check_same_thread,
        cached_statements.

    Notes:
        * After closing an in-memory database, re-connection creates
          a new database, so error is triggered because previous table
          does not exists.

        * In pipeline mode, asides from 'set', only 'get' checks the
          pipeline for uncommitted data.
    """

    PYTYPE_SQLTYPE_MAP = {
        str: 'VARCHAR',
        int: 'INTEGER',
        bool: 'BOOLEAN',
        float: 'REAL',
        bytes: 'BLOB',
        None: 'BLOB',
    }

    def __init__(
        self,
        filename: str = ':memory:',
        *,
        table: str = 'facet',
        access_mode: str = 'c',
        use_pipeline: bool = False,
        key_type=str,
        value_type=None,
        serializer: Union[str, 'BaseSerializer'] = 'json',
        **conn_info,
    ):
        self._conn = None
        self._pipeline = None
        self._name = None
        self._table = table
        self._uri = None
        self._access_mode = access_mode
        self._use_pipeline = use_pipeline
        self._serializer = None
        # Disable serializer if value type is supported by database
        self.serializer = (
            None if value_type in (str, int, bool, float, bytes)
            else serializer
        )
        self._is_connected = False
        self._conn_info = copy.deepcopy(conn_info)

        # Parse file name as if in URI format
        parsed = urllib.parse.urlsplit(filename)
        self._name = parsed.path

        # Prioritize access mode in URI over argument value
        query = parse_address_query(parsed.query)
        mode = query.get('mode')
        if mode:
            if mode == 'ro':
                self._access_mode = access_mode = 'r'
            elif mode == 'rw':
                self._access_mode = access_mode = 'w'
            elif mode == 'rwc':
                self._access_mode = access_mode = 'c'
            else:
                self._access_mode = access_mode = mode
        else:
            if access_mode == 'r':
                query['mode'] = 'ro'
                query['nolock'] = '1'
            elif access_mode == 'w':
                query['mode'] = 'rw'
            elif access_mode in ('c', 'n'):
                query['mode'] = 'rwc'

        # Create path/file for persistent database
        if parsed.path != ':memory:' and mode != 'memory':
            db_dir, db_base = os.path.split(expand_envvars(parsed.path))
            if db_dir and access_mode in ('c', 'n'):
                os.makedirs(db_dir, exist_ok=True)

        # Convert file name to URI format
        self._uri = filename if parsed.scheme else 'file:' + parsed.path
        if query:
            self._uri += '?' + unparse_address_query(query)

        self.connect()

        if access_mode == 'n':
            self.clear()

        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            f"key {type(self).PYTYPE_SQLTYPE_MAP[key_type]} PRIMARY KEY, "
            f"value {type(self).PYTYPE_SQLTYPE_MAP[value_type]});"
        )

    @property
    def serializer(self):
        return self._serializer

    @serializer.setter
    def serializer(self, value: Union[str, 'BaseSerializer']):
        if value is None or isinstance(value, str):
            obj = serializer_map[value]()
        elif isinstance(value, BaseSerializer):
            obj = value
        else:
            raise ValueError(f'invalid serializer, {value}')
        self._serializer = obj

    def __len__(self):
        cur = self._conn.execute(f"SELECT COUNT(key) FROM {self._table};")
        return cur.fetchone()[0]

    def __contains__(self, key: str) -> bool:
        cur = self._conn.execute(
            "SELECT EXISTS("
            f"SELECT value FROM {self._table} "
            "WHERE key=(?));", (key,)
        )
        return bool(cur.fetchone()[0])

    def get_table_memsize(self):
        try:
            cur = self._conn.execute(
                "SELECT SUM(\"pgsize\") FROM \"dbstat\" "
                "WHERE name=(?);", (self._table,)
            )
            return cur.fetchone()[0]
        except sqlite3.ERROR:
            pass

    def get_schema(self):
        cur = self._conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND tbl_name=(?);", (self._table,)
        )
        return cur.fetchone()[0]

    def get_config(self):
        return {
            'name': self._name,
            'table': self._table,
            'uri': self._uri,
            'schema': self.get_schema() if self._is_connected else '',
            'access mode': self._access_mode,
            'memory usage': (
                self.get_table_memsize() if self._is_connected else -1
            ),
            'item count': len(self) if self._is_connected else -1,
            'use pipeline': self._use_pipeline,
            'serializer': get_obj_map_key(self._serializer, serializer_map),
        }

    def get(self, key):
        if self._use_pipeline and key in self._pipeline:
            return self._pipeline[key]
        cur = self._conn.execute(
            f"SELECT value FROM {self._table} "
            "WHERE key=(?);", (key,)
        )
        value = cur.fetchone()
        return value if value is None else self._serializer.loads(value[0])

    def set(self, key, value):
        if self._use_pipeline:
            self._pipeline[key] = value
        else:
            _value = self._serializer.dumps(value)
            self._conn.execute(
                f"INSERT INTO {self._table}(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=(?);",
                (key, _value, _value)
            )

    def bulk_set(self, data: Iterable[Tuple[str, Any]]):
        # NOTE: If data needs serialization, iterate through data
        # twice instead of serializing twice, then duplicate value for
        # UPSERT conflict.
        if isinstance(self._serializer, serializer_map[None]):
            data = map(lambda x: (*x, x[1]), data.items())
        else:
            data = map(
                lambda x: (*x, x[1]),
                map(
                    lambda x: (x[0], self._serializer.dumps(x[1])),
                    data.items()
                )
            )

        self._conn.executemany(
            f"INSERT INTO {self._table}(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=(?);", data
        )

    def keys(self):
        cur = self._conn.execute(f"SELECT key FROM {self._table};")
        return map(lambda row: row[0], cur)

    def items(self, *, chunk=100) -> Iterator[Tuple[str, Any]]:
        cur = self._conn.execute(f"SELECT key, value FROM {self._table};")
        data = cur.fetchmany(chunk)
        while data:
            for k, v in data:
                yield k, v
            data = cur.fetchmany(chunk)

    def delete(self, key):
        self._conn.execute(
            f"DELETE FROM {self._table} "
            "WHERE EXISTS ("
            f"SELECT * FROM {self._table} "
            "WHERE key=(?));", (key,)
        )

    def execute(self, cmd, *args):
        if not args:
            cur = self._conn.execute(cmd)
        else:
            cur = self._conn.execute(cmd, args)
        return cur.fetchall()

    def connect(self):
        if self._is_connected:
            return
        self._conn_info.pop('uri', None)
        self._conn = sqlite3.connect(
            self._uri,
            uri=True,
            **self._conn_info,
        )
        if self._use_pipeline:
            self._pipeline = {}
        self._is_connected = True

    def commit(self):
        if self._is_connected:
            if self._use_pipeline and self._pipeline:
                self.bulk_set(self._pipeline)
                self._pipeline = {}
            if self._conn.in_transaction:
                self._conn.commit()

    def disconnect(self):
        if self._is_connected:
            self._pipeline = None
            self._is_connected = False
            self._conn.close()

    def clear(self):
        if self._use_pipeline:
            self._pipeline = {}
        self._conn.execute(f"DROP TABLE IF EXISTS {self._table};")
