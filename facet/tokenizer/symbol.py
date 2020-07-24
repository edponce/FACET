import re
from .base import BaseTokenizer


__all__ = ['SymbolTokenizer']


class SymbolTokenizer(BaseTokenizer):
    """Symbol-based tokenizer."""

    NAME = 'symbol'

    def __init__(
        self,
        symbols: str = '\t\n\r\f\v .,?!"\'-:;()[]{}',
        *,
        include_symbols: str = None,
        exclude_symbols: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._symbols = None

        # NOTE: Included symbols have priority over excluded ones
        if exclude_symbols:
            symbols = ''.join(set(symbols) - set(exclude_symbols))
        if include_symbols:
            symbols = ''.join(set(symbols) | set(include_symbols))

        # Ensure right bracket does not closes regex in 'tokenize()'.
        self._symbols = re.sub(']', '\\]', symbols)

    def sentencize(self, text):
        yield text

    def tokenize(self, text):
        for match in re.finditer(rf'[^{self._symbols}]*', text):
            token = match.group(0)
            if len(token) > 1 and token not in self.STOPWORDS:
                yield (match.start(), match.end() - 1, token)
