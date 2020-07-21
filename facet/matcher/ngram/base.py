from abc import ABC, abstractmethod
from typing import (
    Tuple,
    Iterable,
)


__all__ = ['BaseNgram']


class BaseNgram(ABC):
    """Provided capabilities to extract N-gram features from a given text.

    Notes:
        * Features are returned as tuples to support hashing, faster
          comparisons (e.g., use as keys in a dictionary).
    """

    @staticmethod
    def _make_feature_set(features: Iterable[str]) -> Tuple[str]:
        """Attach ordinal numbers to n-grams (Chaudhuri et at. 2006)."""
        unique_features = []
        seen_features = set()
        for feature in features:
            for i in range(len(features)):
                _feature = feature + str(i)
                if _feature not in seen_features:
                    unique_features.append(_feature)
                    seen_features.add(_feature)
                    break
        return tuple(unique_features)

    def get_features(self, text: str, *, unique: bool = True) -> Tuple[str]:
        features = self._extract_features(text)
        return type(self)._make_feature_set(features) if unique else features

    @abstractmethod
    def _extract_features(self, text: str) -> Tuple[str]:
        pass
