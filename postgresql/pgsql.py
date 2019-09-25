"""Interfaces to access PostgreSQL databases."""

import os
import logging
import psycopg2
import psycopg2.extras
import urllib.parse
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Tuple, NoReturn


__all__ = ['PostgreSQL', 'PostgreSQLStream']


# Configure logging
formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)
# NOTE: To enable/disable console logging, comment/uncomment the following
# lines of StreamHandler.
# sh = logging.StreamHandler()
# sh.setFormatter(formatter)
# logger.addHandler(sh)
# NOTE: To enable/disable file logging, comment/uncomment the following
# lines of FileHandler.
# fh = RotatingFileHandler(os.path.splitext(__file__)[0] + '.log', maxBytes=65536, backupCount=10)
# fh.setFormatter(formatter)
# logger.addHandler(fh)


class PostgreSQL:
    """Wrapper class for accessing a PostgreSQL database.
    Query results are returned as a list of tuples.

    Args:
        dsn (str): Connection parameters as a connection URI or a key/value
            connection string, see
            https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
            The connection URI is parsed by psycopg2/PostgreSQL using
            simple rules, so if user/password contains delimiter symbols
            used by URI then use either the key/value form or the
            'conn_info' mapping.

        logger (loggin.Logger): Logger object to use for database queries.

    Kwargs:
        conn_info (Dict[str, str]): Connection parameters as key/value
            pairs.

    Precedence of parameters is consistent with psycopg2/PostgreSQL:
        * Parameters can be specified in both dsn and kv-pairs,
          the kv-pairs values will have precedence over the dsn values.
        * Parameters set via environment variables are used when not
          provided by dsn or kv-pairs.
        * Password file (passfile) is used when password is not provided
          explicitly. Default passfile is '~/.pgpass'.
        * Parameters not resolved by any of the above methods will use
          default PostgreSQL values.

    Notes:
        * Requires either dsn or at least one connection-related argument.
        * If 'password' is provided next to 'user' in connection URI,
          it cannot contain the '@' symbol.
        * Option values in connection URI cannot contain the '&' symbol.
        * For multi-option values in key/value connection string,
          use '\\ ' to represent a blank space.
    """

    def __init__(
        self,
        dsn=None,
        *,
        logger: 'logging.Logger' = logger,
        **conn_info
    ):
        # NOTE: psycopg2 does not supports the 'passfile' parameter, so
        # set the corresponding environment variable.
        if 'passfile' in conn_info:
            os.environ['PGPASSFILE'] = os.path.expanduser(
                conn_info.pop('passfile')
            )
        self._conn = psycopg2.connect(
            self._sanitize_dsn_options(dsn),
            connection_factory=psycopg2.extras.LoggingConnection,
            **conn_info
        )
        self._conn.initialize(logger)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __del__(self):
        self.close()

    def _sanitize_dsn_options(self, dsn: str) -> str:
        """Simple sanitizer to convert options to percent-encoded strings.

        Notes:
            * Does not supports the ampersand (&) symbol in option values.
        """
        if dsn:
            dsn, *options = dsn.split('?', 1)
            if options:
                safe_options = []
                for option in options[0].split('&'):
                    name, value = option.split('=', 1)
                    safe_options.append(name + '=' + urllib.parse.quote(value))
                dsn += '?' + '&'.join(safe_options)
        return dsn

    def _execute(self, query: str, vars: Tuple[Any] = None) -> NoReturn:
        """Execute a query. Uses an unnamed psycopg2 cursor.

        Args:
            query (str): Query string. See psycopg2.execute().

            vars (Tuple[Any]): Tuple of value variables corresponding to
                query string '%s' formats. See psycopg2.execute().
        """
        self._cursor.execute(query, vars)

    def _fetch(self, max_rows: int = 0) -> List[Tuple[Any]]:
        """Fetch row results from previous query. Uses an unnamed psycopg2
        cursor.

        Args:
            max_rows (int): Maximum number of rows to fetch from
                previously executed query. A negative or zero value
                will fetch all rows. Default is 0.
        """
        if max_rows == 1:
            rows = self._cursor.fetchone()
        elif max_rows > 1:
            rows = self._cursor.fetchmany(size=max_rows)
        else:
            rows = self._cursor.fetchall()
        return rows

    # NOTE: Subclassing may overwrite execute() and fetch(), so use proxy
    # versions for basic behavior and use of default cursor.
    execute = _execute
    fetch = _fetch

    def commit(self) -> NoReturn:
        """Commit pending transactions to database."""
        self._conn.commit()

    def close(self) -> NoReturn:
        """Close database connection."""
        self._cursor.close()
        self._conn.close()

    def select(
        self,
        table: str,
        columns: str = '*',
        clauses: str = None,
        *,
        rand: bool = False,
        rand_col: str = None,
        unique: bool = False,
        **kwargs
    ) -> List[Tuple[Any]]:
        """Execute a select query.

        Args:
            table (str): Database table.

            columns (str): Database columns string. Default is '*'.

            clauses (str): Clauses to add to query. Default is None.

            rand (bool): Enable/disable randomized results. Default is False.

            rand_col (str): Database column representing unique numeric values
                for each row. Used only with randomization enabled. Specifying
                such column greatly increases the query performance.

            unique (bool): Request distinct rows. Default is False.

        Kwargs:
            Arguments passed directly to fetch().
        """
        max_rows = kwargs.get('max_rows', 0)
        if not rand or (rand and not rand_col):
            query = ' '.join(filter(None, [
                'SELECT {2} {0} FROM {1}'.format(
                    columns,
                    table,
                    'DISTINCT' if unique else '',
                ),
                clauses if clauses else '',
                'ORDER BY random()' if rand else '',
                'LIMIT {}'.format(max_rows) if max_rows else '',
            ]))
        else:
            # Get min/max IDs of random column
            query = 'SELECT min({0}), max({0}) FROM {1}'.format(rand_col, table)
            self._execute(query)
            min_id, max_id = self._fetch()[0]
            stop_id = min(max_rows, max_id) if max_rows else max_id

            query = ' '.join(filter(None, [
                'SELECT {0} FROM '
                '(SELECT {6} {1} + trunc(random() * {2})::integer AS {5} '
                'FROM generate_series(1, {3})) s '
                'JOIN {4} USING ({5})'
                .format(
                    columns,
                    min_id,
                    max_id - min_id,
                    # NOTE: This approach does not guarantees selecting the
                    # requested number of rows because random() may duplicate
                    # values, so generate 1XX% values and then limit result.
                    int(1.5 * stop_id),
                    table,
                    rand_col,
                    'DISTINCT' if unique else '',
                ),
                clauses if clauses else '',
                'LIMIT {}'.format(stop_id),
            ]))
        self.execute(query)
        return self.fetch(**kwargs)

    def seed(self, seed: float) -> NoReturn:
        """Set a seed for next call to random().

        Args:
            seed (float): Value between [-1,1].
        """
        self._execute('SELECT setseed({})'.format(seed))


class PostgreSQLStream(PostgreSQL):
    """Streaming version of PostgreSQL class.
    Query results are returned as a generator of tuples.
    """

    def __init__(self, dsn=None, *, logger=logger, **conn_info):
        self._named_cursors = {}
        super().__init__(dsn, **conn_info)

    def execute(self, query, vars=None, *, name: str = 'stream'):
        """Execute a query. Creates and uses a named psycopg2 cursor.

        Args:
             name (str): Name for named cursor. Default is 'stream'.
        """
        proxy_query = 'DECLARE {} CURSOR FOR {}'.format(name, query)
        self._cursor.execute(proxy_query, vars)
        self._named_cursors[name] = self._conn.cursor(name)

    def fetch(
        self,
        max_rows=0,
        *,
        batch_size=0,
        name: str = 'stream',
    ) -> 'Generator':
        """Execute a query. Uses a named psycopg2 cursor.

        Args:
             batch_size (int): Number of rows for each backend transfer from
                 database while iterating results.

             name (str): Name for named cursor. Default is 'stream'.
        """
        if batch_size > 0:
            self._named_cursors[name].itersize = batch_size
        for i, row in enumerate(self._named_cursors[name], start=1):
            yield row
            if i == max_rows:
                break
        self._close_named_cursor(name)

    def _close_named_cursor(self, name: str = None) -> NoReturn:
        """Close a named cursor.

        Args:
             name (str): Name for named cursor. If not provided, all
                 named cursors are closed.
        """
        if not name:
            for key in self._named_cursors.keys():
                self._named_cursors[key].close()
            self._named_cursors = {}
        elif name in self._named_cursors:
            self._named_cursors.pop(name).close()

    def close(self):
        self._close_named_cursor()
        super().close()
