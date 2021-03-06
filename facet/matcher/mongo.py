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
        db (Dict[str, Any]): Options passed directly to
            'MongoDatabase()'.
            database (str): MongoDB database name for storage.

    Kwargs: Options forwarded to 'BaseSimstring()'.
    """

    NAME = 'mongo-simstring'

    _KEYS = (
        ('ng', pymongo.TEXT),
        ('sz', pymongo.ASCENDING),
    )

    def __init__(
        self,
        *,
        # NOTE: Hijack 'db' parameter from 'BaseMatcher'
        db: Dict[str, Any] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if db is None:
            db = {}

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
            for document in self._db.get(query)
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
                # NOTE: Unique document key for database with pipeline enabled.
                key=(len(features), features),
            )
