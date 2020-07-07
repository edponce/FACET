from .base import BaseNgram
from typing import (
    Any,
    List,
    Iterable,
)


__all__ = ['CharacterNgram']


class CharacterNgram(BaseNgram):
    """Extract character N-gram features."""

    def get_features(self, text) -> List[str]:
        _boundary = self.boundary * (self.n - 1)
        _text = _boundary + text + _boundary
        return type(self)._extract_features(self.n, _text)
