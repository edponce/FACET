from .ngram import CharacterFeatures
from .similarity import CosineSimilarity
from QuickerUMLS.database import DictDatabase
from typing import Union, List, Tuple, NoReturn
from collections import defaultdict


__all__ = ['Simstring']


class Simstring:

    def __init__(self, **kwargs):
        self._db = kwargs.get('database', DictDatabase())
        self._fe = kwargs.get('feature_extractor', CharacterFeatures())
        self._measure = kwargs.get('similarity', CosineSimilarity())
        # self._cache_enabled = kwargs.get('enable_cache', False)

        # NOTE: Keep a multilevel dictionary in memory containing
        # the most recent lookups. Do not use cache if database is an
        # in-memory dictionary.
        # if isinstance(self._db, DictDatabase):
        #     self._max_cache_size = 0
        #     self._cache_db = None
        #     self._cache_enabled = False
        # elif self._cache_enabled:
        #     self._max_cache_size = kwargs.get('max_cache_size', 100)
        #     self._cache_db = DictDatabase()
        #     self._cache_enabled = True
        # self._cache_size = 0

    @property
    def db(self):
        # NOTE: Allow access to database so that methods of interest
        # can be controlled externally. For example, 'sync' (for
        # pipe-enabled databases), 'close', and 'save'.
        return self._db

    def insert(self, string: str) -> NoReturn:
        """Insert string into database."""
        features = self._fe.get_features(string)
        self._db.set(
            str(len(features)),
            # NOTE: Create set here to remove duplicates and not
            # affect the numbers of features extracted.
            {feature: string for feature in set(map(str.lower, features))},
            replace=False,
            unique=True
        )

    def _get_strings(self, size: int, feature: str) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        string = self._db.get(str(size), feature)
        return string if string is not None else []

    # def _cache_string(self, size: int, feature: str, string: str) -> NoReturn:
    #     _size = str(size)
    #     self._cache_db.set(_size, feature, string, replace=False, unique=True)
    #     # NOTE: Need to limit cache size, which items to remove?
    #     self._cache_size += 1

    def search(
        self,
        string: str,
        *,
        alpha: float = 0.7,
        rank: bool = False,
    ) -> Tuple[List[str], List[float]]:
        features = self._fe.get_features(string.lower())
        min_features = self._measure.min_features(len(features), alpha)
        max_features = self._measure.max_features(len(features), alpha)
        similar_strings = [
            similar_string
            for candidate_feature_size in range(min_features, max_features + 1)
            for similar_string in self._overlap_join(features, candidate_feature_size, alpha)
        ]
        similarities = [
            self._measure.similarity(
                features,
                self._fe.get_features(similar_string)
            )
            for similar_string in similar_strings
        ]

        if rank:
            similar_strings, similarities = map(list, zip(
                *sorted(
                    zip(similar_strings, similarities),
                    key=lambda v: v[1],
                    reverse=True
                )
            ))
        return list(zip(similar_strings, similarities))

    def _overlap_join(self, features, candidate_feature_size, alpha):
        query_feature_size = len(features)
        tau = self._measure.min_common_features(
            query_feature_size,
            candidate_feature_size,
            alpha,
        )
        sorted_features = sorted(
            features,
            key=lambda x: len(self._get_strings(candidate_feature_size, x))
        )
        candidate_string_to_matched_count = defaultdict(int)
        results = []

        for feature in sorted_features[:query_feature_size - tau + 1]:
            for s in self._get_strings(candidate_feature_size, feature):
                candidate_string_to_matched_count[s] += 1

        for s in candidate_string_to_matched_count.keys():
            for i in range(query_feature_size - tau + 1, query_feature_size):
                feature = sorted_features[i]
                if s in self._get_strings(candidate_feature_size, feature):
                    candidate_string_to_matched_count[s] += 1
                if candidate_string_to_matched_count[s] >= tau:
                    results.append(s)
                    break
                remaining_feature_count = query_feature_size - i - 1
                if candidate_string_to_matched_count[s] + remaining_feature_count < tau:
                    break
        return results
