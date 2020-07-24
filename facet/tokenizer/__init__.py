from .base import BaseTokenizer
from .nltk import NLTKTokenizer
from .null import NullTokenizer
from .spacy import SpaCyTokenizer
from .symbol import SymbolTokenizer
from .whitespace import WhitespaceTokenizer
from .alphanumeric import AlphaNumericTokenizer
from typing import Union


tokenizer_map = {
    NLTKTokenizer.NAME: NLTKTokenizer,
    SpaCyTokenizer.NAME: SpaCyTokenizer,
    WhitespaceTokenizer.NAME: WhitespaceTokenizer,
    AlphaNumericTokenizer.NAME: AlphaNumericTokenizer,
    SymbolTokenizer.NAME: SymbolTokenizer,
    NullTokenizer.NAME: NullTokenizer,
    None: NullTokenizer,
}


def get_tokenizer(value: Union[str, 'BaseTokenizer']):
    if value is None or isinstance(value, str):
        return tokenizer_map[value]()
    elif isinstance(value, BaseTokenizer):
        return value
    raise ValueError(f'invalid tokenizer, {value}')
