"""FACET package."""


import os
import sys
sys.path.append(os.path.abspath('..'))
from meta import *


__all__ = []


import .utils
import .network
from .factory import FacetFactory
from .facets import (
    Facet,
    UMLSFacet,
)
from .formatter import (
    XMLFormatter,
    CSVFormatter,
    NullFormatter,
    YAMLFormatter,
    JSONFormatter,
    PickleFormatter,
)
from .tokenizer import (
    NLTKTokenizer,
    SpaCyTokenizer,
    BasicTokenizer,
    WhitespaceTokenizer,
)
from .database import (
    DictDatabase,
    RedisDatabase,
    SQLiteDatabase,
    ElasticsearchDatabase,
    ElasticsearchKVDatabase,
)
from .serializer import (
    NullSerializer,
    JSONSerializer,
    YAMLSerializer,
    PickleSerializer,
    StringSerializer,
    StringSJSerializer,
)
from .matcher import (
    Simstring,
    ElasticsearchSimstring,
)
from .matcher.ngram import (
    WordNgram,
    CharacterNgram,
)
from .matcher.similarity import (
    DiceSimilarity,
    ExactSimilarity,
    CosineSimilarity,
    JaccardSimilarity,
    OverlapSimilarity,
    HammingSimilarity,
)
from .scripts.facet import cli
