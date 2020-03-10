# PostgreSQL and PostgreSQLStream

Python3 interfaces to access [PostgreSQL](https://www.postgresql.org) databases.
These are class wrappers over the [psycopg2](https://pypi.org/project/psycopg2)
package and provide a simple API to execute queries. The interface consists of
two classes to support normal (PostgreSQL) and streaming (PostgreSQLStream)
fetch capabilities for query results.


## API

The following are the methods available for both PostgreSQL and PostgreSQLStream
classes. For more details on arguments, data types, and usage see the
docstrings provided in `pgsql.py`.
* `PostgreSQL(dsn=None, logger=logger, **conn_info)`
* `PostgreSQLStream(dsn=None, logger=logger, **conn_info)`
* PostgreSQL - `execute(query, vars=None)`
* PostgreSQLStream - `execute(query, vars=None, name='stream')`
* PostgreSQL - `fetch(max_rows=0)`
* PostgreSQLStream - `fetch(max_rows=0, batch_size=0, name='stream')`
* `commit()`
* `select(table, columns='*', clauses=None, rand=False, rand_col=None, unique=False, **kwargs)`
* `seed(seed)`
* `close()`


## Examples using MIMIC-III

The following are examples that show the PostgreSQL and PostgreSQLStream APIs.
Examples vary in the method used to connect to the database and the query
execution. For more details, see `mimic_pgsql.py`. An example of a passfile is
provided, `pgpass`. In PostgreSQL the default passfile is `~/.pgpass`, so you
can `mv pgpass ~/.pgpass` and `chmod 0600 ~/.pgpass`.


Execute query to count number of rows in a table.
Connect to database with password given in a passfile or environment variable.
```python
>>> import pgsql
>>> with pgsql.PostgreSQL(host='128.219.187.155', dbname='mimic', user='mimic_user', options='--search_path=mimiciii') as db:
>>>     db.execute('SELECT count(*) FROM noteevents')
>>>     num_rows = db.fetch()
>>>     print(num_rows)
```

Query 100 notes.
Connect to database with password given in a passfile or environment variable.
```python
>>> import pgsql
>>> db = pgsql.PostgreSQL(host='128.219.187.155', dbname='mimic', user='mimic_user', options='--search_path=mimiciii')
>>> notes = db.select('noteevents', 'text', max_rows=100)
>>> for note in notes:
>>>     print(note[0])
>>> db.close()
```

Use context manager and query 100 notes.
Connect to database with password given in a passfile or environment variable.
```python
>>> import pgsql
>>> with pgsql.PostgreSQL(host='128.219.187.155', dbname='mimic', user='mimic_user', options='--search_path=mimiciii') as db:
>>>     notes = db.select('noteevents', 'text', max_rows=100)
>>>     for note in notes:
>>>         print(note[0])
```

Use context manager and query 100 notes.
Connect to database using a parameter mapping.
```python
>>> import pgsql
>>> conn_info = {
>>>     'host': '128.219.187.155',
>>>     # 'port': '5432',
>>>     'dbname': 'mimic',
>>>     'user': 'mimic_user',
>>>     # 'password': 'xxxxxxx',  # preferably password should be placed in a passfile
>>>     'options': '--search_path=mimiciii',
>>>     # 'passfile': '/non/default/location/pgpass',  # specify passfile
>>> }
>>> with pgsql.PostgreSQL(**conn_info) as db:
>>>     notes = db.select('noteevents', 'text', max_rows=100)
>>>     for note in notes:
>>>         print(note[0])
```

Use context manager and query 100 notes.
Connect to database using a parameter mapping.
For more information on format of dsn and connection strings, see
[libpq_connection_string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING).
```python
>>> import pgsql
>>> dsn = 'postgresql://mimic_user@128.219.187.155/mimic?options=--search_path=mimiciii'
>>> with pgsql.PostgreSQL(dsn) as db:
>>>     notes = db.select('noteevents', 'text', max_rows=100)
>>>     for note in notes:
>>>         print(note[0])
```

(Streaming mode) Use context manager and query 100 random notes.
Connect to database with password given in a passfile or environment variable:
```python
>>> import pgsql
>>> with pgsql.PostgreSQLStream(host='128.219.187.155', dbname='mimic', user='mimic_user', options='--search_path=mimiciii') as db:
>>>     # db.seed(0.5)  # set a seed to get the same results
>>>     notes = db.select('noteevents', 'text', max_rows=100, rand=True, rand_col='row_id')
>>>     for note in notes:
>>>         print(note[0])
```
