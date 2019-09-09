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

    # def _cache_string(
    #     self,
    #     size: int,
    #     feature: str,
    #     string: str,
    # ) -> NoReturn:
    #     _size = str(size)
    #     self._cache_db.set(
    #         _size,
    #         feature,
    #         string,
    #         replace=False,
    #         unique=True,
    #     )
    #     # NOTE: Need to limit cache size, which items to remove?
    #     self._cache_size += 1

    def _get_strings(self, size: int, feature: str) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        string = self._db.get(str(size), feature)
        return string if string is not None else []

    def insert(self, string: str) -> NoReturn:
        """Insert string into database."""
        features = self._fe.get_features(string)
        self._db.set(
            str(len(features)),
            # NOTE: Create set here to remove duplicates and not
            # affect the numbers of features extracted.
            {feature: string for feature in set(map(str.lower, features))},
            replace=False,
            unique=True,
        )

    def search(
        self,
        query_string: str,
        *,
        alpha: float = 0.7,
        rank: bool = True,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        query_features = self._fe.get_features(query_string.lower())
        min_features = self._measure.min_features(len(query_features), alpha)
        max_features = self._measure.max_features(len(query_features), alpha)
        similar_strings = [
            similar_string
            for candidate_feature_size in range(min_features, max_features + 1)
            for similar_string in self._overlap_join(
                query_features,
                candidate_feature_size,
                # tau
                self._measure.min_common_features(
                    len(query_features),
                    candidate_feature_size,
                    alpha,
                )
            )
        ]

        similarities = [
            self._measure.similarity(
                query_features,
                self._fe.get_features(similar_string.lower()),
            )
            for similar_string in similar_strings
        ]
        strings_and_similarities = list(zip(similar_strings, similarities))
        if rank and len(strings_and_similarities) > 1:
            strings_and_similarities = sorted(
                strings_and_similarities,
                key=lambda ss: ss[1],
                reverse=True,
            )
        return strings_and_similarities

    def _overlap_join(self, query_features, candidate_feature_size, tau):
        strings = {
            feature: self._get_strings(candidate_feature_size, feature)
            for feature in query_features
        }
        query_features = sorted(
            query_features,
            key=lambda feature: len(strings[feature]),
        )

        # Use tau parameter to split sorted features
        tau_split = len(query_features) - tau + 1

        # Frequency dictionary of strings in first half of tau-limited features
        strings_frequency = defaultdict(int)
        for feature in query_features[:tau_split]:
            for string in strings[feature]:
                strings_frequency[string] += 1

        # For strings in frequency dictionary, add frequency in second half
        # of tau-limited features.
        candidate_strings = []
        for string in strings_frequency.keys():
            for i, feature in enumerate(query_features[tau_split:],
                                        start=tau_split):
                if string in strings[feature]:
                    strings_frequency[string] += 1

                # If candidate string has enough frequency count, select it.
                if strings_frequency[string] >= tau:
                    candidate_strings.append(string)
                    break

                # Check if a limit is reached where no further candidates will
                # have enough frequency counts.
                remaining_feature_count = len(query_features) - i - 1
                if strings_frequency[string] + remaining_feature_count < tau:
                    break

        return candidate_strings
