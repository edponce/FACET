from .base_simstring import BaseSimstring
from ..database import ElasticsearchDatabase
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Iterable,
)


__all__ = ['ElasticsearchSimstring']


class ElasticsearchSimstring(BaseSimstring):
    """Elasticsearch implementation of Simstring algorithm.

    Args:
        index (str): Elasticsearch index name for storage.

        db (Dict[str, Any]): Options passed directly to
            'ElasticsearchDatabase()'.

    Kwargs:
        Options passed directly to 'BaseSimstring()'.
    """

    _SETTINGS = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'max_result_window': 10000,
            # disable for bulk processing, enable for real-time
            # 'refresh_interval': -1,
        },
    }

    _MAPPINGS = {
        'mappings': {
            'properties': {
                'term': {
                   'type': 'text',
                   'index': False,
                },
                'ng': {
                   'type': 'text',
                   'norms': False,
                   'similarity': 'boolean',
                },
                'sz': {
                   'type': 'integer',
                   'similarity': 'boolean',
                },
            },
        },
    }

    def __init__(
        self,
        *,
        # NOTE: Hijack 'db' parameter from 'BaseMatcher'
        db: Dict[str, Any] = {},
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db = ElasticsearchDatabase(
            index=db.pop('index', 'facet'),
            index_body={
                # NOTE: Check key, then pop, to allow forming {key: map}.
                **(
                    {'settings': db.pop('settings')}
                    if 'settings' in db
                    else type(self)._SETTINGS
                ),
                **type(self)._MAPPINGS,
            },
            **db,
        )

    def _prepare_query(
        self,
        size: Union[int, Tuple[int, int]],
        features: Union[str, Iterable[str]] = None,
        *,
        max_size: int = None,
    ) -> Dict[str, Any]:
        """Construct Elasticsearch body for Simstring query.

        Args:
            size (int, Tuple[int, int]): Size (or range) to search for strings.
                For range, search on [size[0], size[1]).

            features (str, Iterable[str]): String features for search.
                If None, then search only based on size.
        """
        if isinstance(size, int):
            bool_body = {'must': {'match': {'sz': size}}}
        else:
            bool_body = {'must': {'range': {
                'sz': {'gte': size[0], 'lt': size[1]}
            }}}

        if features is not None:
            if not isinstance(features, str):
                features = ' '.join(features)
            bool_body.update({'filter': {'match': {'ng': features}}})

        if max_size is not None and max_size > 0:
            query = {'size': max_size, 'query': {'bool': bool_body}}
        else:
            query = {'query': {'bool': bool_body}}
        return query

    def get_strings(
        self,
        size: Union[int, Tuple[int, int]],
        feature: Union[str, Iterable[str]] = None,
    ) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        query = self._prepare_query(size, feature)
        return [
            document['_source']['term']
            for document in self.db.get(
                query,
                # NOTE: Unique document key for 'get()' in database.
                key=(size, feature),
            )['hits']['hits']
        ]

    def insert(self, string: str):
        """Insert string into database."""
        features = self._ngram.get_features(string)
        # NOTE: Skip short strings that do not produce any features.
        if features:
            self.db.set(
                {
                    'term': string,
                    'sz': len(features),
                    'ng': features,
                },
                # NOTE: Unique document key for 'set()' in database.
                key=(len(features), features),
            )
