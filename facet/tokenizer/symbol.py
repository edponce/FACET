import re
from .regex import RegexTokenizer


__all__ = ['SymbolTokenizer']


class SymbolTokenizer(RegexTokenizer):
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

        # NOTE: Included symbols have priority over excluded ones
        if exclude_symbols:
            symbols = ''.join(set(symbols) - set(exclude_symbols))
        if include_symbols:
            symbols = ''.join(set(symbols) | set(include_symbols))

        # Ensure right bracket does not closes regex in 'tokenize()'.
        self._symbols = re.sub(']', '\\]', symbols)

    def _sentencize(self, text):
        yield from super()._sentencize(text, regex=r'[^\n]+')

    def _tokenize(self, text):
        yield from super()._tokenize(text, regex=rf'[^{self._symbols}]*')
