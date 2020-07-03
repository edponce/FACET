import re
from .base import BaseTokenizer


__all__ = ['BasicTokenizer']


class BasicTokenizer(BaseTokenizer):
    """Tokenizer with simple effect, combines sequential whitespaces."""

    def sentencize(self, text):
        yield text

    def tokenize(self, text):
        yield (0, len(text) - 1, re.sub(r'\W+', ' ', text))
