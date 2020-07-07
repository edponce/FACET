from abc import ABC, abstractmethod
from typing import (
    List,
    Iterable,
)


__all__ = ['BaseNgram']


class BaseNgram(ABC):
    """Provided capabilities to extract N-gram features from a given text."""

    @staticmethod
    def _extract_features(features: Iterable[str]) -> List[str]:
        # Attach ordinal numbers to n-grams (Chaudhuri et at. 2006)
        unique_features = []
        seen_features = set()
        for feature in features:
            for i in range(len(features)):
                if feature not in seen_features:
                    unique_features.append(feature + str(i))
                    seen_features.add(feature)
                    break
        return unique_features

    @abstractmethod
    def get_features(self, text: str, **kwargs) -> List[str]:
        pass
