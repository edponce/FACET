from .base import BaseNgram
from typing import List


__all__ = ['CharacterNgram']


class CharacterNgram(BaseNgram):
    """Extract character N-gram features.

    Args:
        n (int): Size of features.
    """

    def __init__(self, n: int = 3):
        self._n = n

    def get_features(self, text: str) -> List[str]:
        text = text.strip()
        features = [text[i:i + self._n] for i in range(len(text) - self._n + 1)]
        return type(self)._extract_features(features)
