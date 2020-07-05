from typing import Any, Iterable
from abc import ABC, abstractmethod


__all__ = ['BaseSimilarity']


class BaseSimilarity(ABC):
    """Interface for similarity measures."""

    @abstractmethod
    def min_features(self, length: int, alpha: float) -> int:
        """Minimum number of features for searching similar strings."""
        pass

    @abstractmethod
    def max_features(self, length: int, alpha: float) -> int:
        """Maximum number of features for searching similar strings."""
        pass

    @abstractmethod
    def min_common_features(
        self,
        lengthA: int,
        lengthB: int,
        alpha: float,
    ) -> int:
        """Minimum number of features for approximate dictionary matching."""
        pass

    @abstractmethod
    def similarity(
        self,
        featuresA: Iterable[Any],
        featuresB: Iterable[Any],
    ) -> float:
        """Similarity measure between pair of string features."""
        pass
