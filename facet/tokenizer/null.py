from .base import BaseTokenizer


__all__ = ['NullTokenizer']


class NullTokenizer(BaseTokenizer):
    def sentencize(self, text):
        yield text

    def tokenize(self, text):
        yield (0, len(text) - 1, text)
