import os
import pickle
import shutil
import unicodedata
import plyvel as leveldb
from simstring import simstring
from typing import Set, Tuple


def db_key_encode(s):
    return s.encode('utf-8')

def safe_unicode(s):
    return unicodedata.normalize('NFKD', s)


class SimstringDB:
    """Simstring database reader/writer.

    Args:
        db_file (str): Database filename.

        mode (char): Read/write mode for database handle.
            Valid values are: 'r' and 'w'.

        overwrite (bool): Allow overwriting existing database when in write mode.

        query_type (str): Name of similarity measure for extracting data. Valid values are: 'exact', 'dice', 'cosine', 'jaccard', and 'overlap'. Default is 'cosine'.

        threshold (float): Threshold for similarity measure.
            Default is 0.7.

        ngram_length (int): Unit of character n-grams. Default is 3.

        unicode (bool): Enable/disable Unicode mode for writing database.
            In Unicode, wide characters are used in n-grams.
            Default is False.
    """
    def __init__(self, db_file, mode='r', *,
                 overwrite=False,
                 query_type='cosine',
                 threshold=0.7,
                 ngram_length=3,
                 unicode=False):
        self.__unicode = unicode
        if mode == 'w':
            if os.path.exists(db_file):
                if os.path.isfile(db_file):
                    if overwrite:
                        os.remove(db_file)
                else:
                    raise OSError(f'Database file conflicts with existing file system item')
            else:
                db_dir, db_base = os.path.split(db_file)
                if not db_base:
                    raise OSError(f'No filename was provided for database')
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir)

            # NOTE: https://github.com/chokkan/simstring/blob/master/swig/export.h
            # filename (str): Database filename
            # n (int): Unit of n-grams (3)
            # be (bool): True to represent a begin/end of strings in character n-grams (False)
            # unicode (bool): True to use Unicode mode (False)
            self.db = simstring.writer(
                db_file,
                ngram_length,
                False,
                unicode
            )
        else:
            # Example: https://github.com/chokkan/simstring/blob/master/swig/python/sample_unicode.py
            self.db = simstring.reader(db_file)
            self.db.measure = getattr(simstring, query_type)
            self.db.threshold = threshold

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @classmethod
    def normalize_unicode(cls, s) -> str:
        return unicodedata.normalize('NFKD', s)

    def read(self, term):
        if self.__unicode:
            term = type(self).normalize_unicode(term)
        return self.db.retrieve(term)

    def check(self, term) -> bool:
        """Checks whether a match for term is found in database."""
        if self.__unicode:
            term = type(self).normalize_unicode(term)
        return self.db.check(term)

    def write(self, term):
        if self.__unicode:
            term = type(self).normalize_unicode(term)
        self.db.insert(term)

    def close(self):
        self.db.close()


class LevelDB:
    """LevelDB database reader/writer.

    Args:
        db_dir (str): Database directory.

        mode (char): Read/write mode for database handle.
            Valid values are: 'r' and 'w'.

        overwrite (bool): Allow overwriting existing database when in write mode.

        unicode (bool): Enable/disable Unicode mode for writing database.
            Default is False.
    """
    def __init__(self, db_dir, mode='r', *,
                 overwrite=False,
                 unicode=False):
        self.__unicode = unicode
        if mode == 'w':
            if os.path.exists(db_dir):
                if os.path.isdir(db_dir):
                    if overwrite:
                        shutil.rmtree(db_dir)
                else:
                    raise OSError(f'Database directory conflicts with existing file system item')
            else:
                os.makedirs(db_dir)

        self.db = leveldb.DB(db_dir)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @classmethod
    def normalize_unicode(cls, s) -> str:
        return unicodedata.normalize('NFKD', s)

    @classmethod
    def encode(cls, s) -> bytes:
        return s.encode('utf-8')

    def read(self, key):
        if self.__unicode:
            key = type(self).normalize_unicode(key)

        value = self.db.get(type(self).encode(key))
        # Check if value was pickled when stored
        try:
            value = pickle.loads(value)
        except pickle.UnpicklingError:
            pass
        return value

    def check(self, key) -> bool:
        return self.read(key) != None

    def write(self, key, value):
        if self.__unicode:
            key = type(self).normalize_unicode(key)

        if not isinstance(value, bytes):
            value = pickle.dumps(value)
        self.db.put(type(self).encode(key), value)

    def write_batch(self, items):
        """Write a batch of key/value items.
        The first value of each item is used as the key.
        Multiple values per key are allowed.
        """
        with self.db.write_batch(transaction=True) as wb:
            # Values are stored as tuples because they use less storage
            for key, *value in items:
                if self.__unicode:
                    key = type(self).normalize_unicode(key)
                wb.put(type(self).encode(key), pickle.dumps(tuple(value)))

    def close(self):
        self.db.close()


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
            cuis = self.cui_db.get(db_key_encode(term))
            cuis = pickle.loads(cuis)
        except KeyError:
            cuis = set()

        cuis.add((cui, is_preferred))
        self.cui_db.put(db_key_encode(term), pickle.dumps(cuis))

        try:
            self.semtypes_db.get(db_key_encode(cui))
        except KeyError:
            self.semtypes_db.put(
                db_key_encode(cui), pickle.dumps(set(semtypes))
            )

    def bulk_insert_cui(self, cui_bulk):
        with self.cui_db.write_batch(transaction=True) as wb:
            for term, cui, is_preferred in cui_bulk:
                term = db_key_encode(safe_unicode(term))
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
                term = db_key_encode(safe_unicode(term))
                cui = safe_unicode(cui)
                wb.put(term, pickle.dumps((cui, preferred)))

    def bulk_insert_sty(self, sty_bulk):
        with self.semtypes_db.write_batch(transaction=True) as wb:
            for cui, semtypes in sty_bulk:
                cui = db_key_encode(safe_unicode(cui))
                # wb.put(cui, pickle.dumps(set(semtypes)))
                wb.put(cui, pickle.dumps(semtypes))

    def get(self, term):
        term = safe_unicode(term)
        cuis = pickle.loads(self.cui_db.get(db_key_encode(term)))

        # NOTE: To make it work with QuickerUMLS DB format.
        matches = set()
        for cui, is_preferred in {cuis}:
            sty = self.semtypes_db.get(db_key_encode(cui))
            if sty is not None:
                matches.add((cui,
                             pickle.loads(sty),
                             is_preferred))
        return matches

        # matches = (
        #     (
        #         cui,
        #         pickle.loads(self.semtypes_db.get(db_key_encode(cui))),
        #         is_preferred
        #     )
        #     for cui, is_preferred in cuis
        # )
        # return matches
