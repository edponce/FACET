from .base import BaseNgram
from typing import List


__all__ = ['WordNgram']


class WordNgram(BaseNgram):
    """Extract word N-gram features.

    Args:
        n (int): Size of features.

        delimiter (str): Delimiter symbol for words.

        joiner (str): Symbol to join feature words.
    """
    def __init__(self, n: int = 3, *, delimiter: str = ' ', joiner: str = ' '):
        self._n = n
        self._delim = delimiter
        self._joiner = joiner

    def get_features(self, text: str) -> List[str]:
        words = [x.strip() for x in text.split(self._delim) if x]
        features = [
            self._joiner.join(words[i:i + self._n])
            for i in range(len(words) - self._n + 1)
        ]
        return type(self)._extract_features(features)
