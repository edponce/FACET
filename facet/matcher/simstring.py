from collections import defaultdict
from .base import BaseMatcher
from ..database import (
    database_map,
    BaseDatabase,
)
from .similarity import (
    similarity_map,
    BaseSimilarity,
)
from .ngram import (
    ngram_map,
    BaseNgram,
)
from typing import (
    List,
    Tuple,
    Union,
    NoReturn,
    Iterator,
)


__all__ = ['Simstring']


class Simstring(BaseMatcher):
    """Implementation of Simstring algorithm.

    Okazaki, Naoaki, and Jun'ichi Tsujii. "Simple and efficient algorithm for
    approximate dictionary matching." Proceedings of the 23rd International
    Conference on Computational Linguistics. Association for Computational
    Linguistics, 2010.

    Args:
        db (str, BaseDatabase): Handle to database instance or database name
            for strings storage. Valid databases are: 'dict', 'redis',
            'elasticsearch'.

        cache_db (str, BaseDatabase): Handle to database instance or database
            name for strings cache. Valid databases are: 'dict', 'redis',
            'elasticsearch'.

        alpha (float): Similarity threshold in range (0,1].

        similarity (str, BaseSimilarity): Instance of similarity measure or
            similarity name. Valid measures are: 'cosine', 'jaccard', 'dice',
            'exact', 'overlap', 'hamming'.

        ngram (str, BaseNgram): N-gram feature extractor instance
            or n-gram name. Valid n-gram extractors are: 'word', 'character'.
    """

    _GLOBAL_MAX_FEATURES = 128

    def __init__(
        self,
        *,
        db: Union[str, 'BaseDatabase'] = 'dict',
        cache_db: Union[str, 'BaseDatabase'] = None,
        alpha: float = 0.7,
        similarity: Union[str, 'BaseSimilarity'] = 'jaccard',
        ngram: Union[str, 'BaseNgram'] = 'character',
    ):
        # Key/value store for {feature: terms}
        # NOTE: Actually stored as {str(len(features)) + feature: [terms]}
        self._db = None
        self._cache_db = None
        self._alpha = None
        self._similarity = None
        self._ngram = None

        self.db = db
        self.cache_db = cache_db
        self.alpha = alpha
        self.similarity = similarity
        self.ngram = ngram

        # NOTE: Can track max number of n-gram features when inserting strings
        # into database, but this value will not be available for other
        # processes nor during a later search. Solution is to store the value
        # into the database. But what if the database is not a key-value store,
        # such as ElasticSearch.
        gmf = self._db.get('__GLOBAL_MAX_FEATURES__')
        self._global_max_features = (
            type(self)._GLOBAL_MAX_FEATURES
            if gmf is None
            else gmf
        )
        self._global_max_features = type(self)._GLOBAL_MAX_FEATURES

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, value: Union[str, 'BaseDatabase']):
        if isinstance(value, str):
            obj = database_map[value]()
        elif isinstance(value, BaseDatabase):
            obj = value
        else:
            raise ValueError(f'invalid Simstring database, {value}')
        self._db = obj

    @property
    def cache_db(self):
        return self._cache_db

    @cache_db.setter
    def cache_db(self, value: Union[str, 'BaseDatabase']):
        if isinstance(value, str):
            obj = database_map[value]()
        elif isinstance(value, BaseDatabase):
            obj = value
        else:
            obj = None
        self._cache_db = obj

    @property
    def alpha(self):
        return self._alpha

    @alpha.setter
    def alpha(self, alpha: float):
        # Bound alpha to range [0.01,1]
        self._alpha = min(1, max(alpha, 0.01))

    @property
    def similarity(self):
        return self._similarity

    @similarity.setter
    def similarity(self, value: Union[str, 'BaseSimilarity']):
        if isinstance(value, str):
            obj = similarity_map[value]()
        elif isinstance(value, BaseSimilarity):
            obj = value
        else:
            raise ValueError(f'invalid similarity measure, {value}')
        self._similarity = obj

    @property
    def ngram(self):
        return self._ngram

    @ngram.setter
    def ngram(self, value: Union[str, 'BaseNgram']):
        if isinstance(value, str):
            obj = ngram_map[value]()
        elif isinstance(value, BaseNgram):
            obj = value
        else:
            raise ValueError(f'invalid n-gram feature extractor, {value}')
        self._ngram = obj

    def _get_strings(self, size: int, feature: str) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        strings = self._db.get(str(size) + feature)
        return [] if strings is None else strings

    def insert(self, string: str) -> NoReturn:
        """Insert string into database."""
        features = self._ngram.get_features(string)
        for feature in features:
            strings = self._get_strings(len(features), feature)
            if string not in strings:
                strings.append(string)
                self._db.set(str(len(features)) + feature, strings)

        # Track and store longest sequence of features
        # NOTE: Too many database accesses. Probably it is best to estimate or
        # fix a value or assume inserts occur during the same installation
        # phase and keep track using class variables, then require a "closing"
        # operation to store value into database.
        if len(features) > self._global_max_features:
            self._global_max_features = len(features)
            self._db.set('__GLOBAL_MAX_FEATURES__', self._global_max_features)

    def search(
        self,
        query_string: str,
        *,
        alpha: float = None,
        similarity: Union[str, 'BaseSimilarity'] = None,
        rank: bool = True,
        update_cache: bool = True,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        """Approximate dictionary matching.

        Args:
            alpha (float): Similarity threshold.

            similarity (str, BaseSimilarity): Instance of similarity measure or
                similarity name. Valid measures are: 'cosine', 'jaccard',
                'dice', 'exact', 'overlap', 'hamming'.

            update_cache (bool): If set, then cache database is updated with
                new queries. Default is True.
        """
        if alpha is None:
            alpha = self._alpha
        if similarity is None:
            similarity = self._similarity
        elif isinstance(similarity, str):
            similarity = similarity_map[similarity]()

        # X = string_to_feature(x)
        query_features = self._ngram.get_features(query_string)

        # Check if query string is in cache
        # NOTE: Cached data assumes all Simstring parameters are the same with
        # the exception of 'alpha'.
        query_in_cache = False
        if self._cache_db is not None:
            candidate_strings = self._cache_db.get(str(alpha) + query_string)
            if candidate_strings is not None:
                query_in_cache = True

        if not query_in_cache:
            min_features = max(
                1,
                self._similarity.min_features(len(query_features), alpha)
            )
            max_features = min(
                self._global_max_features,
                self._similarity.max_features(len(query_features), alpha)
            )

            # Y = list of strings similar to the query
            candidate_strings = [
                candidate_string
                # for l in range(min_y(|X|,a), max_y(|X|,a))
                for candidate_feature_size in range(min_features,
                                                    max_features + 1)
                # t = min_overlap(|X|,l,a)
                # for r in overlapjoin(X,t,V,l)
                for candidate_string in self._overlap_join(
                    query_features,
                    candidate_feature_size,
                    self._similarity.min_common_features(
                        len(query_features),
                        candidate_feature_size,
                        alpha,
                    ),
                )
            ]

            # Insert candidate strings into cache
            # NOTE: Need a way to limit database and only cache heavy hitters.
            if update_cache and self._cache_db is not None:
                self._cache_db.set(str(alpha) + query_string, candidate_strings)

        similarities = [
            self._similarity.similarity(
                query_features,
                self._ngram.get_features(candidate_string),
            )
            for candidate_string in candidate_strings
        ]
        strings_and_similarities = list(
            filter(lambda ss: ss[1] >= alpha,
                   zip(candidate_strings, similarities))
        )
        if rank:
            strings_and_similarities.sort(key=lambda ss: ss[1], reverse=True)

        return strings_and_similarities

    def _overlap_join(
        self,
        query_features,
        candidate_feature_size,
        tau,
    ) -> Iterator[str]:
        """CPMerge algorithm with pruning for solving the t-overlap join
        problem."""
        # Sort elements in X by ascending order of |get(V,l,Xk)|
        strings = {
            feature: self._get_strings(candidate_feature_size, feature)
            for feature in query_features
        }
        query_features.sort(key=lambda feature: len(strings[feature]))

        # Use tau parameter to split sorted features
        tau_split = len(query_features) - tau + 1

        # Frequency dictionary of compact set of candidate strings
        # M = {}
        strings_frequency = defaultdict(int)
        # for k in range(|X|-t)
        for feature in query_features[:tau_split]:
            # for s in get(V,l,Xk)
            for string in strings[feature]:
                # M[s] = M[s] + 1
                strings_frequency[string] += 1

        # for k in range(|X|-t+1,|X|-1)
        for i, feature in enumerate(query_features[tau_split:],
                                    start=tau_split):
            prune_strings = []
            # for s in M
            for string in strings_frequency.keys():
                # if bsearch(get(V,l,Xk),s)
                if string in strings[feature]:
                    # M[s] = M[s] + 1
                    strings_frequency[string] += 1

                # If candidate string has enough frequency count, select it.
                # if t <= M[s]
                if strings_frequency[string] >= tau:
                    # R = []
                    # Append s to R
                    yield string
                    # Remove s from M
                    prune_strings.append(string)

                # Prune candidate string if it is found to be unreachable
                # for t overlaps, even if it appears in all of the
                # unexamined inverted lists.
                # if M[s] + (|X|-k-1) < t
                elif (
                      strings_frequency[string]
                      + (len(query_features) - i - 1) < tau
                ):
                    # Remove s from M
                    prune_strings.append(string)

            # Apply pruning
            for string in prune_strings:
                del strings_frequency[string]
