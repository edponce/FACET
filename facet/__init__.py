"""FACET package."""


import os
import sys
sys.path.append(os.path.abspath('..'))
from meta import *


__all__ = []


from .factory import FacetFactory
from .facet import Facet
from .umls import UMLSFacet
from .formatter import (
    JSONFormatter,
    YAMLFormatter,
    XMLFormatter,
    PickleFormatter,
    CSVFormatter,
    SimpleFormatter,
)
from .tokenizer import (
    NLTKTokenizer,
    SpaCyTokenizer,
    SimpleTokenizer,
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
