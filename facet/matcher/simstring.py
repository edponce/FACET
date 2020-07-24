from .base_simstring import BaseSimstring
from ..database import BaseDatabase
from typing import (
    List,
    Union,
)


__all__ = ['Simstring']


class Simstring(BaseSimstring):
    """Key/value store implementation of Simstring algorithm.

    Notes:
        * Key/value store for {feature: terms}, actually stored as
          {str(len(features)) + feature: [terms]}

    Kwargs: Options forwarded to 'BaseSimstring()'.
    """

    NAME = 'simstring'

    def __init__(self, *, db: Union[str, 'BaseDatabase'] = 'dict', **kwargs):
        super().__init__(db=db, **kwargs)

        # NOTE: Can track max number of n-gram features when inserting strings
        # into database, but this value will not be available for other
        # processes nor during a later search. Solution is to store the value
        # into the database. But what if the database is not a key-value store,
        # such as ElasticSearch.
        gmf = self._db.get('__GLOBAL_MAX_FEATURES__')
        self.global_max_features = (
            type(self).GLOBAL_MAX_FEATURES
            if gmf is None
            else gmf
        )

    # def get_strings(self, size: int, feature: str) -> List[str]:
    #     """Get strings corresponding to feature size and query feature."""
    #     strings = self._db.get(str(size) + feature)
    #     return set() if strings is None else strings
    #
    # def insert(self, string: str):
    #     """Insert string into database."""
    #     features = self._ngram.get_features(string)
    #     for feature in features:
    #         strings = self.get_strings(len(features), feature)
    #         if string not in strings:
    #             strings.add(string)
    #             self._db.set(str(len(features)) + feature, strings)
    #
    #     # Track and store longest sequence of features
    #     # NOTE: Too many database accesses. Probably it is best to estimate
    #     # or fix a value or assume inserts occur during the same
    #     # installation phase and keep track using class variables, then
    #     # require a "closing" operation to store value into database.
    #     if len(features) > self.global_max_features:
    #         self.global_max_features = len(features)
    #         self._db.set('__GLOBAL_MAX_FEATURES__', self.global_max_features)

    def get_strings(self, size: int, feature: str) -> List[str]:
        """Get strings corresponding to feature size and query feature."""
        strings = self._db.get(str(size) + feature)
        # NOTE: Explicitly convert into a set to deal with duplicates in
        # 'insert()', but note that we are storing strings as lists to allow
        # JSON/YAML serialization, at the expense of a performance penalty.
        return set() if strings is None else set(strings)

    def insert(self, string: str):
        """Insert string into database."""
        features = self._ngram.get_features(string)
        # NOTE: Skips short strings that do not produce any features.
        for feature in features:
            strings = self.get_strings(len(features), feature)
            if string not in strings:
                strings.add(string)
                # NOTE: Convert set of strings into a list, to make it
                # serializable for databases (e.g., Redis) that use JSON/YAML
                # formats, at the expense of a performance penalty.
                self._db.set(str(len(features)) + feature, list(strings))

        # Track and store longest sequence of features
        # NOTE: Too many database accesses. Probably it is best to estimate or
        # fix a value or assume inserts occur during the same installation
        # phase and keep track using class variables, then require a "closing"
        # operation to store value into database.
        if len(features) > self.global_max_features:
            self.global_max_features = len(features)
            self._db.set('__GLOBAL_MAX_FEATURES__', self.global_max_features)
