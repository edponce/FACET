from .base import BaseTokenizer
from .nltk import NLTKTokenizer
from .spacy import SpaCyTokenizer
from .simple import SimpleTokenizer
from .whitespace import WhitespaceTokenizer


tokenizer_map = {
    'nltk': NLTKTokenizer,
    'spacy': SpaCyTokenizer,
    'ws': WhitespaceTokenizer,
    'none': SimpleTokenizer,
    None: SimpleTokenizer,
}
