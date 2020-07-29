from .regex import RegexTokenizer


__all__ = ['WhitespaceTokenizer']


class WhitespaceTokenizer(RegexTokenizer):
    """Simple whitespace tokenizer."""

    NAME = 'whitespace'

    def _sentencize(self, text):
        yield from super()._sentencize(text, regex=r'[^\n]+')

    def _tokenize(self, text):
        yield from super()._tokenize(text, regex=r'\S+')
