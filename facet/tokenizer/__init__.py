from .base import BaseTokenizer
from .nltk import NLTKTokenizer
from .null import NullTokenizer
from .spacy import SpaCyTokenizer
from .symbol import SymbolTokenizer
from .whitespace import WhitespaceTokenizer
from .alphanumeric import AlphaNumericTokenizer


tokenizer_map = {
    'nltk': NLTKTokenizer,
    'spacy': SpaCyTokenizer,
    'whitespace': WhitespaceTokenizer,
    'alphanumeric': AlphaNumericTokenizer,
    None: NullTokenizer,
    'symbol': SymbolTokenizer,
}
