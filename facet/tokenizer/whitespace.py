import re
from .base import BaseTokenizer


__all__ = ['WhitespaceTokenizer']


class WhitespaceTokenizer(BaseTokenizer):
    """Simple whitespace tokenizer."""

    NAME = 'whitespace'

    def sentencize(self, text):
        yield text

    def tokenize(self, text):
        for match in re.finditer(r'\S+', text):
            token = match.group(0)
            if len(token) > 1 and token not in self.STOPWORDS:
                yield (match.start(), match.end() - 1, token)
