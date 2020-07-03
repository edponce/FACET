"""FACET package."""


import os
import sys
sys.path.append(os.path.abspath('..'))
from meta import *


__all__ = []


from .factory import FacetFactory
from .facets import (
    Facet,
    UMLSFacet,
)
from .formatter import (
    JSONFormatter,
    YAMLFormatter,
    XMLFormatter,
    PickleFormatter,
    CSVFormatter,
    BasicFormatter,
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
    Elasticsearchx,
    ElasticsearchDatabase,
)
from .serializer import (
    JSONSerializer,
    YAMLSerializer,
    PickleSerializer,
    StringSerializer,
    StringSJSerializer,
)
from .simstring import (
    Simstring,
    ElasticsearchSimstring,
)
from .simstring.ngram import (
    WordNgram,
    CharacterNgram,
)
from .simstring.similarity import (
    DiceSimilarity,
    ExactSimilarity,
    CosineSimilarity,
    JaccardSimilarity,
    OverlapSimilarity,
    HammingSimilarity,
)
from .scripts.facet import cli
