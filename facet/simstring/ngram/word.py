from .base import BaseNgram
from typing import (
    Any,
    List,
    Iterable,
)


__all__ = ['WordNgram']


class WordNgram(BaseNgram):
    """Extract word N-gram features."""

    def get_features(self, text, *, delimiter: str = ' ') -> List[List[str]]:
        _boundary = [self.boundary] * (self.n - 1)
        _text = _boundary + text.split(delimiter) + _boundary
        return type(self)._extract_features(self.n, _text)
