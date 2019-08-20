from typing import List, Union


__all__ = ['NgramExtractor']


class NgramExtractor:
    """Provided capabilities to extract N-gram features from a given
    text.

    Args:
        n (int): Size of features.

        boundary (str): Character to use as placeholder for boundary features.
    """
    def __init__(self, n=3, boundary=' '):
        self._n = n
        self._boundary = boundary

    @property
    def n(self):
        return self._n

    @property
    def boundary(self):
        return self._boundary

    def _extract_features(
        self,
        text: Union[str, List[str]],
    ) -> List[Union[str, List[str]]]:
        return [text[i:i + self._n] for i in range(len(text) - self._n + 1)]

    def character_features(self, word: str) -> List[str]:
        """Extract character N-gram features."""
        _text = self._boundary + word + self._boundary
        return self._extract_features(_text)

    def word_features(self, text: str, delimiter=' ') -> List[List[str]]:
        """Extract word N-gram features."""
        _text = [self._boundary] + text.split(delimiter) + [self._boundary]
        return self._extract_features(_text)
