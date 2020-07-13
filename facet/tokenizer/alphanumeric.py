import re
from .base import BaseTokenizer


__all__ = ['AlphaNumericTokenizer']


class AlphaNumericTokenizer(BaseTokenizer):
    """Alphanumeric tokenizer."""

    def tokenize(self, text):
        for match in re.finditer(r'\w+', text):
            token = match.group(0)
            if len(token) > 1 and token not in self.STOPWORDS:
                yield (match.start(), match.end() - 1, token)