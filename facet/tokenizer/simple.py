import re
from .base import BaseTokenizer


__all__ = ['SimpleTokenizer']


class SimpleTokenizer(BaseTokenizer):
    """Tokenizer with no effect."""

    def sentencize(self, text):
        yield text

    def tokenize(self, text):
        yield (0, len(text) - 1, re.sub(r'\W+', ' ', text))
