"""FACET package."""


__title__ = "QuickerUMLS"
__name__ = "QuickerUMLS"
__version__ = "0.9"
__description__ = """Framework for Annotation and Concept Extraction in Text"""
__keywords__ = [
    "information extraction",
    "UMLS",
    "concepts",
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
    'Installer',
    'Formatter',
    'SplitTokenizer',
    'SpacyTokenizer',
    'DictDatabase',
    'RedisDatabase',
    'JSONSerializer',
    'YAMLSerializer',
    'PickleSerializer',
    'StringSerializer',
    'StringSJSerializer',
    'Simstring',
    'WordFeatures',
    'CharacterFeatures',
    'DiceSimilarity',
    'ExactSimilarity',
    'CosineSimilarity',
    'JaccardSimilarity',
    'OverlapSimilarity',
]


from .match import Facet
from .install import Installer
from .formatter import Formatter
from .tokenizer import (
    SplitTokenizer,
    SpacyTokenizer,
)
from .database import (
    DictDatabase,
    RedisDatabase,
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
    WordFeatures,
    CharacterFeatures,
    DiceSimilarity,
    ExactSimilarity,
    CosineSimilarity,
    JaccardSimilarity,
    OverlapSimilarity,
)
