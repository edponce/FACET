from .base import BaseTokenizer


__all__ = ['SplitTokenizer']


class SplitTokenizer(BaseTokenizer):
    """Simple whitespace tokenizer with no sentence segmentation."""

    def sentencize(self, text):
        return [text]

    def tokenize(self, text):
        return [(0, 0, token)
                for token in text.split()
                if token not in type(self).STOPWORDS]
