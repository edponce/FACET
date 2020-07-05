from abc import ABC, abstractmethod
from typing import (
    Any,
    List,
    Iterable,
)


__all__ = ['BaseNgram']


class BaseNgram(ABC):
    """Provided capabilities to extract N-gram features from a given
    text.

    Args:
        n (int): Size of features.

        boundary (str): Character/word to use as padding for boundary features.
    """

    def __init__(self, *, n: int = 3, boundary: str = ' '):
        self.n = n
        self.boundary = boundary

    @staticmethod
    def _extract_features(
        n: int,
        text: Iterable[Iterable[Any]],
    ) -> List[Iterable[Any]]:
        # return [text[i:i + n] for i in range(len(text) - n + 1)]

        # Attach ordinal numbers to n-grams (Chaudhuri et at. 2006)
        features = [text[i:i + n] for i in range(len(text) - n + 1)]
        mod_features = []
        for feature in features:
            for i in range(1, len(features)):
                mod_feature = feature + str(i)
                if mod_feature not in mod_features:
                    mod_features.append(mod_feature)
                    break
        return mod_features[n-1:-n+1]

    @abstractmethod
    def get_features(self, text: str, **kwargs) -> List[Any]:
        pass
