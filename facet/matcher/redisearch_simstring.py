import redisearch
from .base_simstring import BaseSimstring
from ..database import RediSearchDatabase
from typing import (
    Any,
    List,
    Dict,
)


__all__ = ['RediSearchSimstring']


class RediSearchSimstring(BaseSimstring):
    """RediSearch implementation of Simstring algorithm.

    Args:
        index (str): RediSearch index name for storage.

        db (Dict[str, Any]): Options passed directly to
            'RediSearchDatabase()'.

    Kwargs:
        Options passed directly to 'BaseSimstring()'.
    """

    _FIELDS = (
        redisearch.TextField('term', no_stem=True),
        redisearch.TextField('ng', no_stem=False),
        redisearch.NumericField('sz', sortable=True),
    )

    def __init__(
        self,
        *,
        # NOTE: Hijack 'db' parameter from 'BaseMatcher'
        db: Dict[str, Any] = {},
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db = RediSearchDatabase(
            index=db.pop('index', 'facet'),
            fields=type(self)._FIELDS,
            **db,
        )
        # NOTE: Use document count as document IDs
        self._doc_id = len(self.db)

    def get_strings(self, size: int, feature: str) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        query = (
            redisearch.Query(feature)
            .verbatim()
            .limit_fields('ng')
            .add_filter(redisearch.NumericFilter('sz', size, size))
            .return_fields('term')
        )
        return [
            document.term
            for document in self.db.get(query).docs
        ]

    def insert(self, string: str):
        """Insert string into database."""
        features = self._ngram.get_features(string)
        # NOTE: RediSearch does not supports storing lists in a field,
        # so we create a document for each feature. Downside is the high
        # redundancy of data and extra storage.
        for i, feature in enumerate(features):
            self.db.set(
                str(self._doc_id + i),
                {
                    'term': string,
                    'sz': len(features),
                    'ng': feature,
                },
            )
        self._doc_id += len(features)
