import re
from .base import BaseTokenizer


__all__ = ['RegexTokenizer']


class RegexTokenizer(BaseTokenizer):
    """Tokenizer with regex parameter."""

    def _is_valid_token(self, token: str):
        return (
            len(token) >= self._min_token_length
            and token not in self._stopwords
        )

    def _sentencize(self, text, *, regex: str):
        for match in re.finditer(regex, text):
            sentence = match.group(0)
            yield match.start(), match.end() - 1, sentence

    def _tokenize(self, text, *, regex: str):
        for match in re.finditer(regex, text[2]):
            token = match.group(0)
            if self._is_valid_token(token):
                yield (
                    text[0] + match.start(),
                    text[0] + match.end() - 1,
                    token,
                )
