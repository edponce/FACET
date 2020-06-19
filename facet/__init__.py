"""FACET package."""


__title__ = "FACET"
__name__ = "FACET"
__version__ = "0.9"
__description__ = """Framework for Annotation and Concept Extraction in Text"""
__keywords__ = [
    "information extraction",
    "UMLS",
    "concepts",
    "text annotation",
    "medical text",
    "natural language processing",
]
__url__ = "https://github.com/edponce/FACET"
__author__ = "Eduardo Ponce, Oak Ridge National Laboratory, Oak Ridge, TN"
__author_email__ = "edponce2010@gmail.com"
__license__ = "MIT"
__copyright__ = """2020 Eduardo Ponce, Kris Brown, and Edmon Begoli
                   Oak Ridge National Laboratory"""


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
