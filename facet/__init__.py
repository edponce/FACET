"""FACET package."""


import os
import sys
sys.path.append(os.path.abspath('..'))
from meta import *  # noqa: E402


__all__ = []


from .match import Facet
from .match_es import ESFacet
from .install import Installer
from .install_es import ESInstaller
from .formatter import Formatter
from .tokenizer import (
    NLTKTokenizer,
    SpacyTokenizer,
    WhitespaceTokenizer,
)
from .database import (
    DictDatabase,
    RedisDatabase,
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
    ESSimstring,
    WordNgram,
    CharacterNgram,
    DiceSimilarity,
    ExactSimilarity,
    CosineSimilarity,
    JaccardSimilarity,
    OverlapSimilarity,
    HammingSimilarity,
)
from .scripts.facet import cli
