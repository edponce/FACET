from .base import BaseTokenizer


__all__ = ['NullTokenizer']


class NullTokenizer(BaseTokenizer):

    NAME = 'null'

    def _sentencize(self, text):
        yield 0, len(text) - 1, text

    def _tokenize(self, text):
        yield 0, len(text) - 1, text
