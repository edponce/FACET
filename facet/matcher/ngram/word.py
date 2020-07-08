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
        self.n = n
        self.delim = delimiter
        self.joiner = joiner

    def _extract_features(self, text: str) -> List[str]:
        words = [x.strip() for x in text.split(self.delim) if x]
        return [
            self.joiner.join(words[i:i + self.n])
            for i in range(len(words) - self.n + 1)
        ]
