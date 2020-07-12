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
    """implementation of Simstring algorithm.

    Okazaki, Naoaki, and Jun'ichi Tsujii. "Simple and efficient algorithm for
    approximate dictionary matching." Proceedings of the 23rd International
    Conference on Computational Linguistics. Association for Computational
    Linguistics, 2010.

    Args:
        index (str): Elasticsearch database index for storage.

        db (Dict[str, Any]): Options passed directly to
            'ElasticsearchDatabase()'.

    Kwargs:
        Options passed directly to 'BaseSimstring()'.
    """

    _INDICES = [
        ('ng', pymongo.TEXT),
        ('sz', pymongo.ASCENDING),
    ]

    def __init__(
        self,
        *,
        # NOTE: Hijack 'db' parameter from 'BaseMatcher'
        db: Dict[str, Any] = {},
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db = MongoDatabase(
            database=db.pop('database', 'facet'),
            index=type(self)._INDICES,
            **db,
        )

    def get_strings(self, size: int, feature: str) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        query = {'sz': size, 'ng': feature}
        return [
            document['term']
            for document in self.db.get(query)
        ]

    def insert(self, string: str):
        """Insert string into database."""
        features = self._ngram.get_features(string)
        self.db.set({
            'term': string,
            'sz': len(features),
            'ng': features,
        })
