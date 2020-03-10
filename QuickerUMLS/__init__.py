"""FACET package."""


__title__ = "QuickerUMLS"
__name__ = "QuickerUMLS"
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
__url__ = "code.ornl.gov:REACHVET/reachvet-nlp/quicker-umls.git"
__author__ = "Eduardo Ponce, Oak Ridge National Laboratory, Oak Ridge, TN"
__author_email__ = "poncemojicae@ornl.gov"
__license__ = "MIT"
__copyright__ = """2019 Eduardo Ponce, Kris Brown, and Edmon Begoli
                   Oak Ridge National Laboratory"""


__all__ = [
    'Facet',
    'ESFacet',
    'Installer',
    'ESInstaller',
    'Formatter',
    'NLTKTokenizer',
    'SpacyTokenizer',
    'WhitespaceTokenizer',
    'DictDatabase',
    'RedisDatabase',
    'Elasticsearchx',
    'ElasticsearchDatabase',
    'JSONSerializer',
    'YAMLSerializer',
    'PickleSerializer',
    'StringSerializer',
    'StringSJSerializer',
    'Simstring',
    'ESSimstring',
    'WordFeatures',
    'CharacterFeatures',
    'DiceSimilarity',
    'ExactSimilarity',
    'CosineSimilarity',
    'JaccardSimilarity',
    'OverlapSimilarity',
]


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
    WordFeatures,
    CharacterFeatures,
    DiceSimilarity,
    ExactSimilarity,
    CosineSimilarity,
    JaccardSimilarity,
    OverlapSimilarity,
)
