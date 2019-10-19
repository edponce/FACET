from collections import defaultdict
from typing import List, Tuple, Union, NoReturn
from .ngram import CharacterFeatures as Features
from .similarity import CosineSimilarity as Similarity
from QuickerUMLS.database import ElasticsearchDatabase as Database


__all__ = ['ESSimstring']


class ESSimstring:
    """Implementation of Simstring algorithm.

    Args:
        db (BaseDatabase): Database instance for storage.

        feature_extractor (NgramFeatures): N-gram feature extractor instance.

        similarity (BaseSimilarity): Instance of similarity measure.

        case (str): Character to control string casing during insert/search.
            Valid values are 'L' (lower), 'U' (upper), or None (no casing).
            Default is 'L'.
    """

    SETTINGS = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'max_result_window': 10000,
            # disable for bulk processing, enable for real-time
            # 'refresh_interval': -1,
        },
    }

    MAPPINGS = {
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
        db: Union[str, 'ElasticsearchDatabase'],
        feature_extractor: 'NgramFeatures' = Features(),
        similarity: 'BaseSimilarity' = Similarity(),
        case: str = 'L',
    ):
        if isinstance(db, str):
            db = Database(
                index=db,
                body={**type(self).SETTINGS, **type(self).MAPPINGS},
            )
        db.fields = ['sz', 'ng']
        self._db = db
        self._fe = feature_extractor
        self._measure = similarity
        self._case = case

    @property
    def db(self):
        return self._db

    def _get_strings(self, size: int, feature: str) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        meta_hits = self._db.get(size, feature)['hits']
        hits = meta_hits['hits']
        return ([hit['_source']['term'] for hit in hits]
                if len(hits) > 0 else [])

    def _strcase(self, string: str):
        if self._case in ('l', 'L'):
            string = string.lower()
        elif self._case in ('u', 'U'):
            string = string.upper()
        return string

    def insert(self, string: str) -> NoReturn:
        """Insert string into database."""
        features = self._fe.get_features(string)
        self._db.set({
            'term': string,
            'sz': len(features),
            # NOTE: Create set here to remove duplicates and not
            # affect the numbers of features extracted.
            'ng': list(set(map(self._strcase, features))),
        })

    def search(
        self,
        query_string: str,
        *,
        alpha: float = 0.7,
        rank: bool = True,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        query_features = self._fe.get_features(self._strcase(query_string))
        min_features = self._measure.min_features(len(query_features), alpha)
        max_features = self._measure.max_features(len(query_features), alpha)
        similar_strings = [
            similar_string
            for candidate_feature_size in range(min_features, max_features + 1)
            for similar_string in self._overlap_join(
                query_features,
                candidate_feature_size,
                # tau = min_common_features()
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
                self._fe.get_features(self._strcase(similar_string)),
            )
            for similar_string in similar_strings
        ]
        strings_and_similarities = list(
            filter(lambda ss: ss[1] >= alpha,
                   zip(similar_strings, similarities))
        )
        if rank:
            strings_and_similarities.sort(key=lambda ss: ss[1], reverse=True)
        return strings_and_similarities

    def _overlap_join(self, query_features, candidate_feature_size, tau):
        strings = {
            feature: self._get_strings(candidate_feature_size, feature)
            for feature in query_features
        }
        query_features.sort(key=lambda feature: len(strings[feature]))

        # Use tau parameter to split sorted features
        tau_split = len(query_features) - tau + 1

        # Frequency dictionary of strings in first half of tau-limited features
        strings_frequency = defaultdict(int)
        for feature in query_features[:tau_split]:
            for string in strings[feature]:
                strings_frequency[string] += 1

        # TODO: Check if we can use something like this
        # for string in strings_frequency.keys():
        #     for feature in query_features[tau_split:]:
        #         # NOTE: Wouldn't this always be true?
        #         if string in strings[feature]:
        #             strings_frequency[string] += 1

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
