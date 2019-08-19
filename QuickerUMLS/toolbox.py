import os
import pickle
import shutil
import unicodedata
import plyvel as leveldb
from typing import Set, Tuple
from QuickerUMLS.simstring import simstring


def safe_unicode(s):
    return unicodedata.normalize('NFKD', s)


class SimstringDBReader:
    """Simstring read mode database connection.

    Args:
        db_dir (str): Database directory.

        name (str): Database name. Default is 'terms.simstring'.

        measure (str): Similarity measure for extracting data.
            Valid values are: 'exact', 'dice', 'cosine', 'jaccard', and
            'overlap'. Default is 'cosine'.

        threshold (float): Threshold for similarity measure.
            Valid values range in (0, 1]. Default is 0.7.
    """
    def __init__(self, db_dir, *,
                 name='umls-terms.simstring',
                 # db_name='terms.simstring',
                 measure='cosine',
                 threshold=0.7,
                 unicode=False):
        # Bidirectional mapping between measure types and measure IDs
        self.__measures = {
            m: getattr(simstring, m)
            for m in ('exact', 'dice', 'cosine', 'jaccard', 'overlap')
        }
        self.__measures.update(
            {reversed(kv) for kv in self.__measures.items()}
        )

        self.__unicode = unicode
        # https://github.com/chokkan/simstring/blob/master/swig/python/sample_unicode.py
        self.__db = simstring.reader(os.path.join(db_dir, name))
        self.measure = measure
        self.threshold = threshold

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    @classmethod
    def normalize_unicode(cls, s) -> str:
        return unicodedata.normalize('NFKD', s)

    @property
    def measure(self):
        return self.__measures[self.__db.measure]

    @measure.setter
    def measure(self, measure):
        try:
            self.__db.measure = self.__measures[measure]
        except KeyError:
            raise RuntimeError('Invalid Simstring measure type')

    @property
    def threshold(self):
        return self.__db.threshold

    @threshold.setter
    def threshold(self, threshold):
        self.__db.threshold = threshold

    def read(self, term):
        if self.__unicode:
            term = type(self).normalize_unicode(term)
        return self.__db.retrieve(term)

    def check(self, term) -> bool:
        """Checks whether a match for term is found in database."""
        if self.__unicode:
            term = type(self).normalize_unicode(term)
        return self.__db.check(term)

    def close(self):
        self.__db.close()


class SimstringDBWriter:
    """Simstring write mode database connection.

    Args:
        db_dir (str): Database directory.

        name (str): Database name. Default is 'terms.simstring'.

        ngram_length (int): Unit of character n-grams. Default is 3.

        unicode (bool): Enable/disable Unicode mode for writing database.
            In Unicode, wide characters are used in n-grams.
            Default is False.

        overwrite (bool): Allow overwriting existing database when in write
            mode. Default is False.
    """
    def __init__(self, db_dir, *,
                 name='umls-terms.simstring',
                 # db_name='terms.simstring',
                 ngram_length=3,
                 unicode=False,
                 overwrite=False):
        if os.path.exists(db_dir):
            if os.path.isdir(db_dir):
                if overwrite:
                    shutil.rmtree(db_dir)
                    os.makedirs(db_dir)
            else:
                raise OSError('Database directory conflicts with existing '
                              'file system item')
        else:
            os.makedirs(db_dir)

        self.__unicode = unicode
        # https://github.com/chokkan/simstring/blob/master/swig/export.h
        # filename (str): Database filename
        # n (int): Unit of n-grams (3)
        # be (bool): True to represent a begin/end of strings in character
        #   n-grams (False)
        # unicode (bool): True to use Unicode mode (False)
        self.__db = simstring.writer(
            os.path.join(db_dir, name), ngram_length, False, unicode
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    @classmethod
    def normalize_unicode(cls, s) -> str:
        return unicodedata.normalize('NFKD', s)

    def write(self, term):
        if self.__unicode:
            term = type(self).normalize_unicode(term)
        self.__db.insert(term)

    def write_batch(self, terms):
        for term in terms:
            if self.__unicode:
                term = type(self).normalize_unicode(term)
            self.__db.insert(term)

    def close(self):
        self.__db.close()


class LevelDB:
    """LevelDB database reader/writer.

    Args:
        db_dir (str): Database directory.

        overwrite (bool): Allow overwriting existing database when in write
            mode.
    """
    # NOTE: Add custom serializer option, will require generic except handling
    def __init__(self, db_dir, *,
                 overwrite=False):
        if os.path.exists(db_dir):
            if os.path.isdir(db_dir):
                if overwrite:
                    # NOTE: Removes entire database directory
                    shutil.rmtree(db_dir)
                    os.makedirs(db_dir)
            else:
                raise OSError('Database directory conflicts with existing '
                              'file system item')
        else:
            os.makedirs(db_dir)

        self.__db = leveldb.DB(db_dir, create_if_missing=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    @classmethod
    def encode(cls, s) -> bytes:
        return s.encode('utf-8')

    def read(self, key):
        # NOTE: Should return None if key is not found
        value = self.__db.get(type(self).encode(key))
        if value is not None:
            # Check if value was pickled when stored
            try:
                value = pickle.loads(value)
            except pickle.UnpicklingError:
                pass
        return value

    def check(self, key) -> bool:
        return self.read(key) is not None

    def write(self, key, value):
        if not isinstance(value, bytes):
            value = pickle.dumps(value)
        self.__db.put(type(self).encode(key), value)

    def write_batch(self, items):
        """Write a batch of key/value items.
        The first value of each item is used as the key.
        Multiple values per key are allowed.
        """
        with self.__db.write_batch(transaction=True) as wb:
            # Values are stored as tuples because they use less storage
            for key, *value in items:
                wb.put(type(self).encode(key), pickle.dumps(tuple(value)))

    def close(self):
        self.__db.close()


class UMLSDB:
    """Database layer for UMLS CUIs and semantic types.

    Args:
        db_dir (str): Database directory.

        overwrite (bool): Allow overwriting existing database when in write
            mode.

        unicode (bool): Enable/disable Unicode mode for writing database.
            Default is False.
    """
    def __init__(self, db_dir, *,
                 cui_name='cui.leveldb',
                 sty_name='semtypes.leveldb',
                 # cui_name='cui.db',
                 # sty_name='sty.db',
                 overwrite=False,
                 unicode=False):
        self.__unicode = unicode
        self.cui_db = LevelDB(
            os.path.join(db_dir, cui_name),
            overwrite=overwrite
        )
        self.sty_db = LevelDB(
            os.path.join(db_dir, sty_name),
            overwrite=overwrite
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __write(self, db, key, value, overwrite):
        if overwrite:
            db.write(key, value)
        else:
            # Keys can have multiple associated values, so extend previously
            # stored values.
            try:
                values = db.read(key)
                if value not in values:
                    values += value,
                    db.write(key, values)
            except KeyError:
                db.write(key, (value,))

    def __del__(self):
        self.close()

    @classmethod
    def normalize_unicode(cls, s) -> str:
        return unicodedata.normalize('NFKD', s)

    def write_cui(self, term, cui, overwrite=False):
        if self.__unicode:
            term = type(self).normalize_unicode(term)
        self.__write(self.cui_db, term, cui, overwrite)

    def write_sty(self, cui, sty, overwrite=False):
        self.__write(self.sty_db, cui, sty, overwrite)

    def write_batch_cui(self, batch):
        self.cui_db.write_batch(batch)

    def write_batch_sty(self, batch):
        self.sty_db.write_batch(batch)

    def read(self, term):
        cuis = self.cui_db.read(term)
        if cuis is None:
            return ()

        matches = set()
        for cui, *value in {cuis}:
            print(cui, *value)
            print(type(cui), type(value))
            sty = self.sty_db.read(cui)
            if sty is not None:
                matches.add((cui, sty, *value))
        return tuple(matches)

        # matches = (
        #     (cui, self.sty_db.read(cui), *value) for cui, *value in {cuis}
        # )
        # return matches

    def close(self):
        self.cui_db.close()
        self.sty_db.close()


class CuiSemTypesDB:
    def __init__(self, path):
        if not (os.path.exists(path) or os.path.isdir(path)):
            err_msg = (
                '"{}" is not a valid directory').format(path)
            raise IOError(err_msg)

        self.cui_db = leveldb.DB(
            os.path.join(path, 'cui.leveldb'), create_if_missing=True)
        self.semtypes_db = leveldb.DB(
            os.path.join(path, 'semtypes.leveldb'), create_if_missing=True)

    def insert(self, term, cui, semtypes, is_preferred):
        term = safe_unicode(term)
        cui = safe_unicode(cui)

        # some terms have multiple cuis associated with them,
        # so we store them all
        try:
            cuis = self.cui_db.get(term.encode())
            cuis = pickle.loads(cuis)
        except KeyError:
            cuis = set()

        cuis.add((cui, is_preferred))
        self.cui_db.put(term.encode(), pickle.dumps(cuis))

        try:
            self.semtypes_db.get(cui.encode())
        except KeyError:
            self.semtypes_db.put(
                cui.encode(), pickle.dumps(set(semtypes))
            )

    def bulk_insert_cui(self, cui_bulk):
        with self.cui_db.write_batch(transaction=True) as wb:
            for term, cui, is_preferred in cui_bulk:
                term = safe_unicode(term).encode()
                cui = safe_unicode(cui)

                # Some terms have multiple cuis associated with them,
                # so store them all
                # NOTE: get() searches for data in disk, not in write_batch.
                cuis = self.cui_db.get(term)
                if cuis is None:
                    cuis = set()
                else:
                    cuis = pickle.loads(cuis)

                cuis.add((cui, is_preferred))
                wb.put(term, pickle.dumps(cuis))

    def safe_bulk_insert_cui(self, cui_bulk):
        with self.cui_db.write_batch(transaction=True) as wb:
            for term, cui, preferred in cui_bulk:
                term = safe_unicode(term).encode()
                cui = safe_unicode(cui)
                wb.put(term, pickle.dumps((cui, preferred)))

    def bulk_insert_sty(self, sty_bulk):
        with self.semtypes_db.write_batch(transaction=True) as wb:
            for cui, semtypes in sty_bulk:
                cui = safe_unicode(cui).encode()
                # wb.put(cui, pickle.dumps(set(semtypes)))
                wb.put(cui, pickle.dumps(semtypes))

    def get(self, term):
        term = safe_unicode(term)
        cuis = pickle.loads(self.cui_db.get(term.encode()))
        print(cuis)

        # NOTE: To make it work with QuickerUMLS DB format.
        matches = set()
        for cui, is_preferred in {cuis}:
            print(cui)
            sty = self.semtypes_db.get(cui.encode())
            print(sty)
            if sty is not None:
                matches.add((cui,
                             pickle.loads(sty),
                             is_preferred))
        return matches

        # matches = (
        #     (
        #         cui,
        #         pickle.loads(self.semtypes_db.get(cui.encode())),
        #         is_preferred
        #     )
        #     for cui, is_preferred in cuis
        # )
        # return matches
