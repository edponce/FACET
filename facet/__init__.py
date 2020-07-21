import os
import sys
sys.path.append(os.path.abspath('..'))
from meta import *


__all__ = []


# NOTE: These imports are for CLI script
from .configuration import Configuration
from .helpers import (
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
    NullTokenizer,
    NLTKTokenizer,
    SpaCyTokenizer,
    SymbolTokenizer,
    WhitespaceTokenizer,
    AlphaNumericTokenizer,
)
from .database import (
    DictDatabase,
    RedisDatabase,
    RediSearchDatabase,
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
    RediSearchSimstring,
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
