import redisearch
from .base import BaseMatcher
from .base_simstring import BaseSimstring
from ..database import (
    RediSearchDatabase,
    RediSearchAutoCompleterDatabase,
)
from .similarity import get_alpha
from .distance import get_distance as get_similarity
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
)


__all__ = [
    'RediSearchSimstring',
    'RediSearch',
    'RediSearchAutoCompleter',
]


class RediSearchSimstring(BaseSimstring):
    """RediSearch implementation of Simstring algorithm.

    Args:
        db (Dict[str, Any]): Options passed directly to
            'RediSearchDatabase()'.
            index (str): RediSearch index name for storage.

    Kwargs: Options forwarded to 'BaseSimstring()'.
    """

    NAME = 'redisearch-simstring'

    _FIELDS = (
        redisearch.TextField('term', no_stem=True),
        redisearch.TextField('ng', no_stem=False),
        redisearch.NumericField('sz', sortable=True),
    )

    def __init__(
        self,
        *,
        # NOTE: Hijack 'db' parameter from 'BaseMatcher'
        db: Dict[str, Any] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if db is None:
            db = {}

        self._db = RediSearchDatabase(
            index=db.pop('index', 'facet'),
            fields=type(self)._FIELDS,
            **db,
        )
        # NOTE: Use document count as document IDs
        self._doc_id = len(self._db)

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
            for document in self._db.get(query).docs
        ]

    def insert(self, string: str):
        """Insert string into database."""
        features = self._ngram.get_features(string)
        # NOTE: RediSearch does not supports storing lists in a field,
        # so we create a document for each feature. Downside is the high
        # redundancy of data and extra storage.
        for i, feature in enumerate(features):
            self._db.set(
                str(self._doc_id + i),
                {
                    'term': string,
                    'sz': len(features),
                    'ng': feature,
                },
            )
        self._doc_id += len(features)


class RediSearch(BaseMatcher):
    """RediSearch.

    Args:
        alpha (float): Similarity threshold in range (0,1].

        similarity (str, BaseSimilarity): Similarity measure instance or name.

        db (Dict[str, Any]): Options passed directly to
            'RediSearchDatabase()'.
            index (str): RediSearch index name for storage.

    Kwargs: Options forwarded to 'BaseMatcher()'.
    """

    NAME = 'redisearch'

    _FIELDS = (redisearch.TextField('term'),)

    def __init__(
        self,
        *,
        alpha: float = 0.7,
        similarity: str = None,
        # NOTE: Hijack 'db' parameter from 'BaseMatcher'
        db: Dict[str, Any] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._alpha = None
        self._similarity = None

        self.alpha = alpha
        self.similarity = similarity

        if db is None:
            db = {}

        self._db = RediSearchDatabase(
            index=db.pop('index', 'facet'),
            fields=type(self)._FIELDS,
            **db,
        )
        # NOTE: Use document count as document IDs
        self._doc_id = len(self._db)

    @property
    def alpha(self):
        return self._alpha

    @alpha.setter
    def alpha(self, alpha: float):
        self._alpha = alpha

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

    def insert(self, string: str):
        """Insert string into database."""
        self._db.set(str(self._doc_id), {'term': string})
        self._doc_id += 1

    def search(
        self,
        string: str,
        *,
        alpha: float = None,
        similarity: str = None,
        rank: bool = True,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        """Approximate dictionary matching.

        Args:
            alpha (float): Similarity threshold.

            similarity (str): Instance of similarity measure or
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

        # NOTE: Cached data assumes approximate string matching parameters
        # (similariy measure) are the same with the exception of 'alpha'
        # because results may differ. Therefore, do not use cache database
        # if similarity measure from argument differs from internal
        # similarity measure.
        use_cache = (
            similarity == self._similarity
            and self._cache_db is not None
        )

        # Check if query string is in cache
        if use_cache:
            strings_and_similarities = self._cache_db.get(string)
            if strings_and_similarities is not None:
                return strings_and_similarities

        # List of strings similar to the query
        candidate_strings = [
            document.term
            for document in self._db.get(string).docs
        ]

        similarities = [
            self._similarity(string, candidate_string)
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
            self._cache_db.set(string, strings_and_similarities)

        return strings_and_similarities


class RediSearchAutoCompleter(BaseMatcher):
    """RediSearch AutoCompleter.

    Args:
        db (Dict[str, Any]): Options passed directly to
            'RediSearchAutoCompleterDatabase()'.
            key (str): RediSearch index name for storage.

    Kwargs: Options forwarded to 'BaseMatcher()'.
    """

    NAME = 'redisearch-autocompleter'

    def __init__(
        self,
        *,
        # NOTE: Hijack 'db' parameter from 'BaseMatcher'
        db: Dict[str, Any] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if db is None:
            db = {}

        self._db = RediSearchAutoCompleterDatabase(
            key=db.pop('key', 'facet-ac'),
            **db,
        )

    def insert(self, suggestion: Union[str, redisearch.Suggestion], **kwargs):
        """Insert string into database."""
        self._db.set(suggestion, **kwargs)

    def search(self, string: str) -> Union[List[Tuple[str, float]], List[str]]:
        # Check if query string is in cache
        if self._cache_db is not None:
            strings_and_similarities = self._cache_db.get(string)
            if strings_and_similarities is not None:
                return strings_and_similarities

        strings_and_similarities = [
            # NOTE: No matching score is provided by RediSearch, so we
            # set to 1 to ensure that all matches are considered as valid.
            (suggestion.string, suggestion.score)
            for suggestion in self._db.get(
                string,
                fuzzy=True,
                with_scores=True,
            )
        ]

        # Insert candidate strings into cache
        # NOTE: Need a way to limit database and only cache heavy hitters.
        if self._cache_db is not None:
            self._cache_db.set(string, strings_and_similarities)

        return strings_and_similarities
