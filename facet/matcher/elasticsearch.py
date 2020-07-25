from .base import BaseMatcher
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


__all__ = [
    'ElasticsearchSimstring',
    'ElasticsearchFuzzy',
]


class ElasticsearchSimstring(BaseSimstring):
    """Elasticsearch implementation of Simstring algorithm.

    Args:
        index (str): Elasticsearch index name for storage.

        db (Dict[str, Any]): Options passed directly to
            'ElasticsearchDatabase()'.

    Kwargs: Options forwarded to 'BaseSimstring()'.
    """

    NAME = 'elasticsearch-simstring'

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
        self._db = ElasticsearchDatabase(
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
            for document in self._db.get(query)['hits']['hits']
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


class ElasticsearchFuzzy(BaseMatcher):
    """Elasticsearch string matching.

    Args:
        index (str): Elasticsearch database index for storage.

        db (Dict[str, Any]): Options passed directly to
            'ElasticsearchDatabase()'.

    Kwargs: Options forwarded to 'BaseMatcher()'.
    """

    NAME = 'elasticsearch-fuzzy'

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
                   'index': True,
                },
            },
        },
    }

    def __init__(
        self,
        *,
        # NOTE: Hijack 'db' parameter from 'BaseMatcher'
        db: Dict[str, Any] = {},
        rank: bool = True,
        exact_match: bool = False,
        fuzziness: str = 'AUTO',
        prefix_length: int = 0,
        max_expansions: int = 50,
        transpositions: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # NOTE: Find a better way to set these parameters. Updating dict
        # for every search is not ideal.
        self._search_opts = {
            'rank': rank,
            'exact_match': exact_match,
            'fuzziness': fuzziness,
            'prefix_length': prefix_length,
            'max_expansions': max_expansions,
            'transpositions': transpositions,
        }

        self._db = ElasticsearchDatabase(
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
        string: str,
        *,
        rank: bool = True,
        exact_match: bool = False,
        fuzziness: str = 'AUTO',
        prefix_length: int = 0,
        max_expansions: int = 50,
        transpositions: bool = True,
    ) -> Dict[str, Any]:
        """Construct Elasticsearch body for exact/fuzzy matching."""
        if exact_match:
            query_body = {
                'match': {
                    'term': string
                }
            }
        else:
            query_body = {
                'fuzzy': {
                    'term': {
                        'value': string,
                        'fuzziness': fuzziness,
                        'prefix_length': prefix_length,
                        'max_expansions': max_expansions,
                        'transpositions': transpositions,
                    }
                }
            }

        query = {'sort': [{'_score': 'desc'}]} if rank else {}
        query.update({'query': query_body})
        return query

    def insert(self, string: str):
        self._db.set({'term': string})

    def search(
        self,
        string: str,
        **kwargs,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        # Check if query string is in cache
        if self._cache_db is not None:
            strings_and_similarities = self._cache_db.get(string)
            if strings_and_similarities is not None:
                return strings_and_similarities

        query = self._prepare_query(
            string,
            **{**self._search_opts, **kwargs},
        )
        strings_and_similarities = [
            (document['_source']['term'], document['_score'])
            for document in self._db.get(query)['hits']['hits']
        ]

        # Insert candidate strings into cache
        # NOTE: Need a way to limit database and only cache heavy hitters.
        if self._cache_db is not None:
            self._cache_db.set(string, strings_and_similarities)

        return strings_and_similarities
