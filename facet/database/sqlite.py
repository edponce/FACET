import os
import sqlite3
import urllib.parse
from .base import BaseKVDatabase
from ..serializer import (
    get_serializer,
    BaseSerializer,
)
from ..helpers import (
    parse_address_query,
    unparse_address_query,
    expand_envvars,
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

        connect (bool): If set, automatically connect during initialization.

        key_type (type): Python-based type for keys in database.
            Valid values are: 'str', 'int', 'bool', 'float', 'bytes'.
            Internally, sqlite3 converts keys to the selected type.

        value_type (type): Python-based type for values in database.
            Valid values are: 'str', 'int', 'bool', 'float', 'bytes'.
            If None, then any type is allowed and the serializer is used for
            converting values to binary form. Use 'bytes' when you are already
            providing binary data.

        serializer (str, BaseSerializer): Serializer instance or serializer
            name.

    Kwargs: Options forwarded to 'sqlite3.connect()'. See
        https://docs.python.org/3/library/sqlite3.html#sqlite3.connect

    Notes:
        * By default, read-only mode disables database locks and thread
          checks, and enables shared cache.

        * In pipeline mode, asides from 'set', only 'get' checks the
          pipeline for uncommitted data.
    """

    NAME = 'sqlite'

    # Converts a SQLite URI mode into traditional access mode flags.
    _SQLITEMODE_ACCESSMODE_MAP = {
        'ro': 'r',
        'rw': 'w',
        'rwc': 'c',
        'memory': 'w',
    }

    # Converts a traditional access mode flag into a SQLite URI mode.
    _ACCESSMODE_SQLITEMODE_MAP = {
        'r': 'ro',
        'w': 'rw',
        'c': 'rwc',
        'n': 'rwc',
    }

    _PYTYPE_SQLTYPE_MAP = {
        str: 'VARCHAR',
        int: 'INTEGER',
        bool: 'BOOLEAN',
        float: 'REAL',
        bytes: 'BLOB',
    }

    def __init__(
        self,
        uri: str = ':memory:',
        *,
        table: str = 'test',
        access_mode: str = 'c',
        use_pipeline: bool = False,
        connect: bool = True,
        key_type=str,
        value_type=None,
        serializer: Union[str, 'BaseSerializer'] = 'pickle',
        **conn_info,
    ):
        self._conn = None
        self._pipeline = None
        self._uri = uri
        self._path = None
        self._table = table
        self._access_mode = access_mode
        self._use_pipeline = use_pipeline
        self._key_type = key_type
        self._value_type = value_type
        self._serializer = serializer
        self._conn_info = conn_info

        if connect:
            self.connect()
        else:
            self._pre_connect(make_dir=False)

    def _pre_connect(self, make_dir: bool = True, **kwargs):
        self._uri = expand_envvars(kwargs.pop('uri', self._uri))
        self._table = expand_envvars(kwargs.pop('table', self._table))
        self._access_mode = kwargs.pop('access_mode', self._access_mode)
        self._use_pipeline = kwargs.pop('use_pipeline', self._use_pipeline)
        self._key_type = kwargs.pop('key_type', self._key_type)
        self._value_type = kwargs.pop('value_type', self._value_type)
        self._serializer = kwargs.pop('serializer', self._serializer)
        self._conn_info.update(kwargs)

        # Parse file name as if in URI format
        parsed = urllib.parse.urlsplit(self._uri)
        self._path = parsed.path
        query = parse_address_query(parsed.query)

        # Prioritize access mode in URI over argument value
        if 'mode' in query:
            self._access_mode = (
                type(self)._SQLITEMODE_ACCESSMODE_MAP[query['mode']]
            )
        else:
            query['mode'] = (
                type(self)._ACCESSMODE_SQLITEMODE_MAP[self._access_mode]
            )

        # Custom settings for read-only mode
        if query['mode'] == 'ro':
            query['nolock'] = query.get('nolock', '1')
            query['cache'] = query.get('cache', 'shared')
            self._conn_info['check_same_thread'] = (
                self._conn_info.get('check_same_thread', False)
            )

        # Create path/file for persistent database
        if self._path != ':memory:' and query['mode'] != 'memory':
            db_dir, db_base = os.path.split(self._path)
            if make_dir and db_dir and self._access_mode in ('c', 'n'):
                os.makedirs(db_dir, exist_ok=True)

        # Convert configuration to URI format
        self._uri = 'file:{}?{}'.format(
            self._path,
            unparse_address_query(query),
        )

        # Disable serializer if value type is supported by database
        self._serializer = get_serializer(
            'null' if self._value_type in type(self)._PYTYPE_SQLTYPE_MAP
            else self._serializer
        )

    def _post_connect(self):
        if self._access_mode in ('c', 'n'):
            if self._access_mode == 'n':
                self.clear()

            key_type = (
                type(self)._PYTYPE_SQLTYPE_MAP.get(self._key_type, 'BLOB')
            )
            value_type = (
                type(self)._PYTYPE_SQLTYPE_MAP.get(self._value_type, 'BLOB')
            )
            self._conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table} "
                f"(key {key_type} PRIMARY KEY, "
                f"value {value_type});"
            )

        if self._use_pipeline:
            self._pipeline = {}

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

    @property
    def backend(self):
        return self._conn

    def table_memsize(self):
        cur = self._conn.execute(
            "SELECT SUM(\"pgsize\") FROM \"dbstat\" "
            "WHERE name=(?);", (self._table,)
        )
        return cur.fetchone()[0]

    def schema(self):
        cur = self._conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND tbl_name=(?);", (self._table,)
        )
        return cur.fetchone()[0]

    def configuration(self):
        is_connected = self.ping()
        return {
            'connected': is_connected,
            'uri': self._uri,
            'path': self._path,
            'table': self._table,
            'schema': self.schema() if is_connected else '',
            'access mode': self._access_mode,
            'pipelined': self._use_pipeline,
            'serializer': type(self._serializer).NAME,
            'nrows': len(self) if is_connected else -1,
            'store': (
                self.table_memsize() if is_connected else -1
            ),
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
        if type(self._serializer).NAME == 'null':
            data = map(lambda x: (*x, x[1]), data.items())
        else:
            data = map(lambda x: (*x, x[1]),
                       map(lambda x: (x[0], self._serializer.dumps(x[1])),
                           data.items()))

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
        cur = self._conn.execute(cmd, args)
        return cur.fetchall()

    def connect(self, **kwargs):
        if self.ping():
            return

        self._pre_connect(**kwargs)

        # NOTE: Discard 'uri' option because we always use URI format.
        self._conn_info.pop('uri', None)
        self._conn = sqlite3.connect(self._uri, uri=True, **self._conn_info)

        self._post_connect()

    def commit(self):
        if not self.ping():
            return
        if self._use_pipeline and self._pipeline:
            self.bulk_set(self._pipeline)
            self._pipeline = {}
        if self._conn.in_transaction:
            self._conn.commit()

    def disconnect(self):
        if self.ping():
            self._pipeline = None
            self._conn.close()

    def clear(self):
        if self._conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND tbl_name=(?);", (self._table,)
        ).fetchall():
            self._conn.execute(f"DELETE FROM {self._table};")
        if self._use_pipeline:
            self._pipeline = {}

    def drop_table(self):
        self._conn.execute(f"DROP TABLE IF EXISTS {self._table};")
        if self._use_pipeline:
            self._pipeline = {}

    def ping(self):
        try:
            if hasattr(self._conn, 'in_transaction'):
                self._conn.in_transaction
                return True
            else:
                return False
        except sqlite3.ProgrammingError:
            return False
