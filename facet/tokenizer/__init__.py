from .base import BaseTokenizer
from .nltk import NLTKTokenizer
from .spacy import SpaCyTokenizer
from .basic import BasicTokenizer
from .whitespace import WhitespaceTokenizer


tokenizer_map = {
    'nltk': NLTKTokenizer,
    'spacy': SpaCyTokenizer,
    'ws': WhitespaceTokenizer,
    'basic': BasicTokenizer,
    None: BasicTokenizer,
}
