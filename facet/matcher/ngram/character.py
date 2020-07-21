from .base import BaseNgram
from typing import Tuple


__all__ = ['CharacterNgram']


class CharacterNgram(BaseNgram):
    """Extract character N-gram features.

    Args:
        n (int): Size of features.

        boundary_length (int): Number of boundary characters to use.
            A negative value, corresponds to (n-1) symbols.

        boundary_symbol (bool): Character/word to use as padding for
            boundary features.
    """

    def __init__(
        self,
        n: int = 3,
        *,
        boundary_length: int = 0,
        boundary_symbol: str = ' ',
    ):
        self.n = n
        self.boundary_length = boundary_length
        self.boundary_symbol = boundary_symbol

    def _extract_features(self, text: str) -> Tuple[str]:
        text = text.strip()
        if self.boundary_length != 0:
            boundary = ' ' * (
                (self.n - 1)
                if self.boundary_length < 0
                else self.boundary_length
            )
            text = boundary + text + boundary
        return tuple(text[i:i + self.n] for i in range(len(text) - self.n + 1))
