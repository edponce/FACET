import re
from .base import BaseTokenizer


__all__ = ['BasicTokenizer']


class BasicTokenizer(BaseTokenizer):
    """Tokenizer that only combines consecutive whitespaces."""

    def sentencize(self, text):
        yield text

    def tokenize(self, text):
        yield (0, len(text) - 1, re.sub(r'\W+', ' ', text))
