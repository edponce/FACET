from collections import defaultdict
from .ngram import CharacterFeatures as Features
from .similarity import CosineSimilarity as Similarity
from QuickerUMLS.database import DictDatabase as Database
from typing import (
    List,
    Tuple,
    Union,
    NoReturn,
    Iterator,
)


__all__ = ['Simstring']


class Simstring:
    """Implementation of Simstring algorithm.

    Args:
        db (BaseDatabase): Database instance for strings storage.

        feature_extractor (NgramFeatures): N-gram feature extractor instance.

        similarity (BaseSimilarity): Instance of similarity measure.

        case (str): Character to control string casing during insert/search.
            Valid values are 'L' (lower), 'U' (upper), or None (no casing).
            Default is 'L'.

        cache_db (BaseDatabase): Database instance for strings cache.
    """
    def __init__(
        self,
        *,
        db: 'BaseDatabase' = Database(),
        feature_extractor: 'NgramFeatures' = Features(),
        similarity: 'BaseSimilarity' = Similarity(),
        case: str = 'L',
        cache_db: 'BaseDatabase' = None,
    ):
        self._db = db
        self._fe = feature_extractor
        self._measure = similarity
        self._case = case
        self._cache_db = cache_db

        # NOTE: Can track max number of n-gram features when inserting strings
        # into database, but this value will not be available for other
        # processes nor during a later search. Solution is to store the value
        # into the database. But what if the database is not a key-value store,
        # such as ElasticSearch.
        self._global_max_features = self._db.get('__GLOBAL_MAX_FEATURES__')

    @property
    def db(self):
        return self._db

    def _get_strings(self, size: int, feature: str) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        strings = self._db.get(str(size), feature)
        return strings if strings is not None else []

    def _strcase(self, string: str):
        if self._case in ('l', 'L'):
            string = string.lower()
        elif self._case in ('u', 'U'):
            string = string.upper()
        return string

    def insert(self, string: str) -> NoReturn:
        """Insert string into database."""
        features = self._fe.get_features(string)
        self._db.set(
            str(len(features)),
            # NOTE: Create set here to remove duplicates and not
            # affect the numbers of features extracted.
            {feature: self._strcase(string)
             for feature in set(map(self._strcase, features))},
            replace=False,
            unique=True,
        )

        # Limit number of candidate features by longest sequence of features
        if (
            self._global_max_features is None
            or len(features) > self._global_max_features
        ):
            self._global_max_features = len(features)
            self._db.set('__GLOBAL_MAX_FEATURES__', self._global_max_features)

    def search(
        self,
        query_string: str,
        *,
        alpha: float = 0.7,
        rank: bool = True,
        update_cache: bool = True,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        """Approximate dictionary matching.

        Args:
            update_cache (bool): If set, then cache database is updated with
                new queries. Default is True.
        """
        _query_string = self._strcase(query_string)

        # X = string_to_feature(x)
        query_features = self._fe.get_features(_query_string)
        # DEBUG
        print(query_features, len(query_features))

        # Check if query string is in cache
        # NOTE: Cached data assumes all Simstring parameters are the same with
        # the exception of 'alpha'.
        query_in_cache = False
        if self._cache_db is not None:
            candidate_strings = self._cache_db.get(_query_string, alpha)[0]
            if candidate_strings is not None:
                query_in_cache = True

        if not query_in_cache:
            min_features = max(
                1,
                self._measure.min_features(len(query_features), alpha)
            )
            max_features = min(
                self._global_max_features,
                self._measure.max_features(len(query_features), alpha)
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
                    self._measure.min_common_features(
                        len(query_features),
                        candidate_feature_size,
                        alpha,
                    )
                )
            ]

            # Insert candidate strings into cache
            if update_cache and self._cache_db is not None:
                self._cache_db.set(_query_string, alpha, candidate_strings)

        # DEBUG
        for candidate_string in candidate_strings:
            f = self._fe.get_features(self._strcase(candidate_string))
            print(f, len(f))

        similarities = [
            self._measure.similarity(
                query_features,
                self._fe.get_features(self._strcase(candidate_string)),
            )
            for candidate_string in candidate_strings
        ]
        strings_and_similarities = list(
            filter(lambda ss: ss[1] >= alpha,
                   zip(candidate_strings, similarities))
        )
        if rank:
            strings_and_similarities.sort(key=lambda ss: ss[1], reverse=True)

        # DEBUG
        print(strings_and_similarities)
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
