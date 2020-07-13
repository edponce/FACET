from .base_simstring import BaseMatcher
from ..database import ElasticsearchDatabase
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
)


__all__ = ['ElasticsearchFuzzy']


class ElasticsearchFuzzy(BaseMatcher):
    """Elasticsearch string matching.

    Args:
        index (str): Elasticsearch database index for storage.

        db (Dict[str, Any]): Options passed directly to
            'ElasticsearchDatabase()'.

    Kwargs:
        Options passed directly to 'BaseMatcher()'.
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
        self.db.set({'term': string})

    def search(
        self,
        string: str,
        **kwargs,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        # Check if query string is in cache
        # NOTE: Cached data assumes all Simstring parameters are the same with
        # the exception of 'alpha'.
        query_in_cache = False
        if self.cache_db is not None:
            strings_and_similarities = self.cache_db.get(string)
            if strings_and_similarities is not None:
                query_in_cache = True

        if not query_in_cache:
            query = self._prepare_query(
                string,
                **{**self._search_opts, **kwargs},
            )
            strings_and_similarities = [
                (document['_source']['term'], document['_score'])
                for document in self.db.get(query)['hits']['hits']
            ]

            # Insert candidate strings into cache
            # NOTE: Need a way to limit database and only cache heavy hitters.
            if self.cache_db is not None:
                self.cache_db.set(string, strings_and_similarities)

        return strings_and_similarities
