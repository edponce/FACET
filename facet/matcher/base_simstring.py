from abc import abstractmethod
from collections import defaultdict
from .base import BaseMatcher
from .similarity import (
    get_similarity,
    get_alpha,
    BaseSimilarity,
)
from .ngram import (
    get_ngram,
    BaseNgram,
)
from typing import (
    List,
    Tuple,
    Union,
    Iterator,
)


__all__ = ['BaseSimstring']


class BaseSimstring(BaseMatcher):
    """Base class for Simstring implementations.

    Okazaki, Naoaki, and Jun'ichi Tsujii. "Simple and efficient algorithm for
    approximate dictionary matching." Proceedings of the 23rd International
    Conference on Computational Linguistics. Association for Computational
    Linguistics, 2010.

    Args:
        alpha (float): Similarity threshold in range (0,1].

        similarity (str, BaseSimilarity): Similarity measure instance or name.

        ngram (str, BaseNgram): N-gram feature extractor instance or name.

    Kwargs: Options forwarded to 'BaseMatcher()'.
    """

    GLOBAL_MAX_FEATURES = 64

    def __init__(
        self,
        *,
        alpha: float = 0.7,
        similarity: Union[str, 'BaseSimilarity'] = 'jaccard',
        ngram: Union[str, 'BaseNgram'] = 'character',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._alpha = None
        self._similarity = None
        self._ngram = get_ngram(ngram)
        self.global_max_features = type(self).GLOBAL_MAX_FEATURES

        self.alpha = alpha
        self.similarity = similarity

    @abstractmethod
    def get_strings(self, size: int, features: str) -> List[str]:
        pass

    @abstractmethod
    def insert(self, string: str):
        pass

    @property
    def alpha(self):
        return self._alpha

    @alpha.setter
    def alpha(self, alpha: float):
        self._alpha = get_alpha(alpha)

    @property
    def similarity(self):
        return self._similarity

    @similarity.setter
    def similarity(self, similarity):
        # NOTE: Clear cache database if similarity measure changes because
        # results may differ.
        if self._cache_db is not None and self._cache_db.ping():
            self._cache_db.clear()
        self._similarity = get_similarity(similarity)

    @property
    def ngram(self):
        return self._ngram

    def search(
        self,
        string: str,
        *,
        alpha: float = None,
        similarity: Union[str, 'BaseSimilarity'] = None,
        rank: bool = True,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        """Approximate dictionary matching.

        Args:
            alpha (float): Similarity threshold.

            similarity (str, BaseSimilarity): Instance of similarity measure or
                similarity name.
        """
        alpha = (
            self._alpha
            if alpha is None
            else get_alpha(alpha)
        )
        similarity = (
            self._similarity
            if similarity is None
            else get_similarity(similarity)
        )

        # NOTE: Cached data assumes Simstring parameters (ngram and
        # similariy measure) are the same with the exception of 'alpha'
        # because results may differ. Therefore, do not use cache database
        # if similarity measure from argument differs from internal
        # similarity measure.
        use_cache = (
            similarity.NAME == self._similarity.NAME
            and self._cache_db is not None
        )

        # Check if query string is in cache
        if use_cache:
            cache_key = str(alpha) + string
            strings_and_similarities = self._cache_db.get(cache_key)
            if strings_and_similarities is not None:
                return strings_and_similarities

        # X = string_to_feature(x)
        query_features = self._ngram.get_features(string)

        min_features = max(
            1,
            similarity.min_features(len(query_features), alpha)
        )
        max_features = min(
            self.global_max_features,
            similarity.max_features(len(query_features), alpha)
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
                similarity.min_common_features(
                    len(query_features),
                    candidate_feature_size,
                    alpha,
                ),
            )
        ]

        similarities = [
            similarity.similarity(
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

        # Insert candidate strings into cache
        # NOTE: Need a way to limit database and only cache heavy hitters.
        if use_cache:
            cache_key = str(alpha) + string
            self._cache_db.set(cache_key, strings_and_similarities)

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
            feature: self.get_strings(candidate_feature_size, feature)
            for feature in query_features
        }
        query_features = sorted(
            query_features,
            key=lambda feature: len(strings[feature]),
        )

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

        # NOTE: I think if the following loops are exchanged, there is no
        # need to do pruning.
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
