"""FACET package."""


import os
import sys
sys.path.append(os.path.abspath('..'))
from meta import *


__all__ = []


from .utils import (
    load_configuration,
    get_obj_map_key,
    parse_address,
)
from .network import (
    SocketClient,
    SocketServer,
    SocketServerHandler,
)
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
    SymbolTokenizer,
    WhitespaceTokenizer,
    AlphaNumericTokenizer,
)
from .database import (
    DictDatabase,
    RedisDatabase,
    SQLiteDatabase,
    MongoDatabase,
    ElasticsearchDatabase,
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
    MongoSimstring,
    ElasticsearchSimstring,
    ElasticsearchFuzzy,
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
