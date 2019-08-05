import os
import pickle
import unicodedata
import plyvel as leveldb
from simstring import simstring


def safe_unicode(s):
    return u'{}'.format(unicodedata.normalize('NFKD', s))


def db_key_encode(term):
    return term.encode('utf-8')


class SimstringDBWriter(object):
    def __init__(self, path):

        if not(os.path.exists(path)) or not(os.path.isdir(path)):
            err_msg = (
                '"{}" does not exists or it is not a directory.'
            ).format(path)
            raise IOError(err_msg)
        else:
            try:
                os.makedirs(path)
            except OSError:
                pass

        self.db = simstring.writer(
            os.path.join(path, 'umls-terms.simstring'),
            3, False, True
        )

    def insert(self, term):
        term = safe_unicode(term)
        self.db.insert(term)


class SimstringDBReader(object):
    def __init__(self, path, similarity_name, threshold):
        if not(os.path.exists(path)) or not(os.path.isdir(path)):
            err_msg = (
                '"{}" does not exists or it is not a directory.'
            ).format(path)
            raise IOError(err_msg)

        self.db = simstring.reader(
            os.path.join(path, 'umls-terms.simstring')
        )
        self.db.measure = getattr(simstring, similarity_name)
        self.db.threshold = threshold

    def get(self, term):
        term = safe_unicode(term)
        return self.db.retrieve(term)


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

    def has_term(self, term):
        term = safe_unicode(term)
        try:
            self.cui_db.get(db_key_encode(term))
            return True
        except KeyError:
            return

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
