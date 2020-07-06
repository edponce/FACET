import os
import sqlite3
import urllib.parse
from .base2 import BaseKVDatabase
from ..utils import (
    parse_filename,
    parse_address_query,
    unparse_address_query,
)
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


__all__ = [
    'SQLite_KVDatabase',
    'SQLiteDatabase',
]


class SQLite_KVDatabase(BaseKVDatabase):
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

    Kwargs (see 'sqlite3.connect()'):
        timeout
        detect_types
        isolation_level
        check_same_thread
        cached_statements

    Notes:
        * After closing an in-memory database, re-connection creates
          a new database, so error is triggered because previous table
          does not exists.
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
        key_type=str,
        value_type=None,
        serializer: Union[str, 'BaseSerializer'] = 'json',
        **kwargs,
    ):
        self._db = None
        self._name = None
        self._table = table
        self._access_mode = None
        self._uri = None
        self._query = None
        self._serializer = None
        self._is_connected = False
        self._conn_params = kwargs

        # Parse file name as if in URI format
        parsed = urllib.parse.urlsplit(filename)
        self._name = parsed.path

        # Prioritize query mode in URI over argument value
        query = parse_address_query(parsed.query)
        self._query = query
        query_mode = query.get('mode')
        if query_mode:
            if query_mode == 'ro':
                access_mode = 'r'
            elif query_mode == 'rw':
                access_mode = 'w'
            elif query_mode == 'rwc':
                access_mode = 'c'
            else:
                # NOTE: If invalid access mode, keep it so that
                # sqlite3 triggers error.
                access_mode = query_mode

        # Create path/file for persistent database
        if parsed.path != ':memory:' and query_mode != 'memory':
            db_dir, db_base = parse_filename(parsed.path)
            if access_mode == 'c':
                os.makedirs(db_dir, exist_ok=True)
            elif access_mode == 'n':
                if os.path.isfile(parsed.path):
                    os.remove(parsed.path)
                else:
                    os.makedirs(db_dir, exist_ok=True)

        # Convert file name to URI format
        self._uri = filename if parsed.scheme else 'file:' + parsed.path

        # Disable serializer if value type is supported by sqlite3
        self.serializer = (
            None if value_type in (str, int, bool, float, bytes)
            else serializer
        )

        self.connect(access_mode=access_mode)

        # Create table if not exists
        self._db.execute(
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

    def __contains__(self, key: str) -> bool:
        cur = self._db.execute(
            "SELECT EXISTS("
            f"SELECT value FROM {self._table} "
            f"WHERE key='{key}');"
        )
        return bool(cur.fetchone()[0])

    def __len__(self):
        cur = self._db.execute(f"SELECT COUNT(key) FROM {self._table};")
        return cur.fetchone()[0]

    def get_table_memsize(self):
        # NOTE: Check for connection status to allow invoking
        # on a closed database.
        if self._is_connected:
            try:
                cur = self._db.execute(
                    "SELECT SUM(\"pgsize\") FROM \"dbstat\" "
                    f"WHERE name='{self._table}';"
                )
                return cur.fetchone()[0]
            except sqlite3.ERROR:
                pass
        return -1

    def execute(self, cmd):
        cur = self._db.execute(cmd)
        return cur.fetchall()

    def get_schema(self):
        cur = self._db.execute(
            f"SELECT sql FROM sqlite_master "
            f"WHERE type='table' AND tbl_name='{self._table}';"
        )
        return cur.fetchone()[0]

    def get_config(self):
        return {
            'name': self._name,
            'uri': self._uri,
            'table': self._table,
            'access mode': self._access_mode,
            'memory usage': (
                self.get_table_memsize() if self._is_connected else -1
            ),
            'item count': len(self) if self._is_connected else -1,
            'schema': self.get_schema() if self._is_connected else '',
            'serializer': self._serializer,
        }

    def get(self, key):
        cur = self._db.execute(
            f"SELECT value FROM {self._table} "
            f"WHERE key='{key}';"
        )
        value = cur.fetchone()
        if value is None:
            raise KeyError(f'{key}')
        return self._serializer.loads(value[0])

    def set(self, key, value, **kwargs):
        _value = self._serializer.dumps(value)
        cur = self._db.execute(
            f"INSERT INTO {self._table}(key, value) VALUES (?, ?) "
            f"ON CONFLICT(key) DO UPDATE SET value=(?);",
            (key, _value, _value)
        )

    def keys(self):
        cur = self._db.execute(f"SELECT key FROM {self._table};")
        return map(lambda row: row[0], cur)

    def items(self) -> Iterator[Tuple[str, Any]]:
        cur = self._db.execute(f"SELECT key, value FROM {self._table};")
        data = cur.fetchmany(100)
        while data:
            for k, v in data:
                yield k, v
            data = cur.fetchmany(100)

    def delete(self, key):
        if key in self:
            self._db.execute(f"DELETE FROM {self._table} WHERE key='{key}';")

    def connect(self, *, access_mode='w'):
        if self._is_connected:
            if self._access_mode == access_mode:
                return
            self._db.close()

        # Add/change access mode options to URI
        query_mode = self._query.get('mode')
        if query_mode != 'memory':
            if access_mode == 'r':
                self._query['mode'] = 'ro'
                self._query['nolock'] = '1'
            elif access_mode == 'w':
                self._query['mode'] = 'rw'
            elif access_mode in ('c', 'n'):
                self._query['mode'] = 'rwc'

        # Update query in URI
        db_uri = self._uri.split('?')[0]
        query = unparse_address_query(self._query)
        self._uri = db_uri + '?' + query if query else db_uri

        self._db = sqlite3.connect(self._uri, uri=True, **self._conn_params)
        self._access_mode = access_mode
        self._is_connected = True

    def commit(self):
        # NOTE: Check for connection status to allow invoking
        # on a closed database.
        if self._is_connected and self._db.in_transaction:
            self._db.commit()

    def disconnect(self):
        if self._is_connected:
            self._db.close()
            self._is_connected = False

    def clear(self):
        self._db.execute(f"DELETE FROM {self._table};")


def SQLiteDatabase(*args, **kwargs):
    return SQLite_KVDatabase(*args, **kwargs)
