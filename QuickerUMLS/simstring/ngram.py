from typing import Any, List, Iterable
from abc import ABC, abstractmethod


__all__ = ['CharacterFeatures', 'WordFeatures']


class NgramFeatures(ABC):
    """Provided capabilities to extract N-gram features from a given
    text.

    Args:
        n (int): Size of features.

        boundary (str): Character to use as placeholder for boundary features.
    """

    def __init__(self, n=3, *, boundary=''):
        self.n = n
        self.boundary = boundary

    @staticmethod
    def _extract_features(
        n: int,
        text: Iterable[Iterable[Any]],
    ) -> List[Iterable[Any]]:
        return [text[i:i + n] for i in range(len(text) - n + 1)]

    @abstractmethod
    def get_features(self, text: str, **kwargs) -> List[Any]:
        pass


class CharacterFeatures(NgramFeatures):
    """Extract character N-gram features."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_features(self, text) -> List[str]:
        _text = self.boundary + text + self.boundary
        return type(self)._extract_features(self.n, _text)


class WordFeatures(NgramFeatures):
    """Extract word N-gram features."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_features(self, text, *, delimiter=' ') -> List[List[str]]:
        _text = [self.boundary] + text.split(delimiter) + [self.boundary]
        return type(self)._extract_features(self.n, _text)
