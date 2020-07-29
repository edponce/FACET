from .regex import RegexTokenizer


__all__ = ['AlphaNumericTokenizer']


class AlphaNumericTokenizer(RegexTokenizer):
    """Alphanumeric tokenizer."""

    NAME = 'alphanumeric'

    def _sentencize(self, text):
        yield from super()._sentencize(text, regex=r'[^\n]+')

    def _tokenize(self, text):
        yield from super()._tokenize(text, regex=r'\w+')
