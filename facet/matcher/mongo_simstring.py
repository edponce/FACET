import pymongo
from .base_simstring import BaseSimstring
from ..database import MongoDatabase
from typing import (
    Any,
    List,
    Dict,
)


__all__ = ['MongoSimstring']


class MongoSimstring(BaseSimstring):
    """MongoDB implementation of Simstring algorithm.

    Args:
        database (str): MongoDB database name for storage.

        db (Dict[str, Any]): Options passed directly to
            'MongoDatabase()'.

    Kwargs: Options forwarded to 'BaseSimstring()'.
    """

    NAME = 'mongo-simstring'

    _KEYS = (
        ('ng', pymongo.TEXT),
        ('sz', pymongo.ASCENDING),
    )

    def __init__(self, *, db: Dict[str, Any] = {}, **kwargs):
        super().__init__(**kwargs)
        self._db = MongoDatabase(
            database=db.pop('database', 'facet'),
            keys=type(self)._KEYS,
            **db,
        )

    def get_strings(self, size: int, feature: str) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        query = {'sz': size, 'ng': feature}
        return [
            document['term']
            for document in self._db.get(
                query,
                # NOTE: Unique document key for 'get()' in database.
                key=(size, feature),
            )
        ]

    def insert(self, string: str):
        """Insert string into database."""
        features = self._ngram.get_features(string)
        # NOTE: Skip short strings that do not produce any features.
        if features:
            self._db.set(
                {
                    'term': string,
                    'sz': len(features),
                    'ng': features,
                },
                # NOTE: Unique document key for 'set()' in database.
                key=(len(features), features),
            )
