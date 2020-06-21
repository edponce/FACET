from .base import BaseTokenizer
from .none import NoneTokenizer
from .nltk import NLTKTokenizer
from .spacy import SpacyTokenizer
from .whitespace import WhitespaceTokenizer


tokenizer_map = {
    'nltk': NLTKTokenizer,
    'spacy': SpacyTokenizer,
    'ws': WhitespaceTokenizer,
    None: NoneTokenizer,
}
