from typing import Any, Iterable
from abc import ABC, abstractmethod


__all__ = ['BaseSimilarity']


class BaseSimilarity(ABC):

    @abstractmethod
    def min_features(self, length: int, alpha: float) -> int:
        pass

    @abstractmethod
    def max_features(self, length: int, alpha: float) -> int:
        pass

    @abstractmethod
    def min_common_features(self,
                            lengthA: int,
                            lengthB: int,
                            alpha: float) -> int:
        pass

    @abstractmethod
    def similarity(self,
                   featuresA: Iterable[Any],
                   featuresB: Iterable[Any]) -> float:
        pass
