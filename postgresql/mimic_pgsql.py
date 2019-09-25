"""MIMIC-III PostgreSQL
Rows in noteevents: 2,083,180
Columns in noteevents: row_id, subject_id, hadm_id, chartdate, charttime,
                       storetime, category, description, cgid, iserror,
                       text, mimic_id
"""

import pgsql


def example1():
    """This version requires database credentials/options to be in passfile
    '~/.pgpass' and/or environment variables.
    """
    # with pgsql.PostgreSQL(host='128.219.187.155', dbname='mimic', user='mimic_user', options='--search_path=mimiciii') as db:
    with pgsql.PostgreSQLStream(host='128.219.187.155', dbname='mimic', user='mimic_user', options='--search_path=mimiciii') as db:
        notes = db.select('noteevents', 'text', max_rows=10)
        # db.seed(0.5)  # specify seed when requesting random rows
        # notes = db.select('noteevents', 'text', max_rows=10, rand=True)
        # notes = db.select('noteevents', 'text', max_rows=10, rand=True, rand_col='row_id')
        for i, note in enumerate(notes):
            if i == 0:
                print(note[0][:80])


def example2():
    """This version uses a key/value mapping to specify connection
    credentials/options."""
    conn_info = {
        'host': '128.219.187.155',
        # 'port': '5432',
        'dbname': 'mimic',
        'user': 'mimic_user',
        # 'password': 'xxxxxxx',  # preferably password should be placed in a passfile
        'options': '--search_path=mimiciii',
        # 'passfile': '/non/default/location/pgpass',  # specify passfile
    }
    with pgsql.PostgreSQL(**conn_info) as db:
        notes = db.select('noteevents', 'text', max_rows=10)
        print(notes[0][0][:80])


def example3():
    """This version uses the connection URI."""
    dsn = 'postgresql://mimic_user@128.219.187.155/mimic?options=--search_path=mimiciii'
    # dsn = 'postgresql://mimic_user@128.219.187.155:5432/mimic?password=xxxxxxx&options=--search_path=mimiciii'
    with pgsql.PostgreSQL(dsn) as db:
        notes = db.select('noteevents', 'text', max_rows=10)
        print(notes[0][0][:80])


def example4():
    """This version uses the key/value connection string."""
    dsn = 'host=128.219.187.155 dbname=mimic user=mimic_user options=--search_path=mimiciii'
    # dsn = 'host=128.219.187.155 port=5432 dbname=mimic user=mimic_user options=--search_path=mimiciii password=xxxxxxx'
    with pgsql.PostgreSQL(dsn) as db:
        notes = db.select('noteevents', 'text', max_rows=10)
        print(notes[0][0][:80])


def example5():
    """Execute a query."""
    dsn = 'postgresql://mimic_user@128.219.187.155/mimic?options=--search_path=mimiciii'
    with pgsql.PostgreSQL(dsn) as db:
        db.execute('SELECT count(*) FROM noteevents')
        num_rows = db.fetch()
        print(num_rows)


if __name__ == '__main__':
    example1()
    example2()
    example3()
    example4()
    example5()
