"""FACET package."""


__title__ = "FACET"
__name__ = "FACET"
__version__ = "0.9"
__description__ = """High-performance tool for concept extraction
                     from medical narratives."""
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
__copyright__ = """2019 Eduardo Ponce and Kris Brown
                   Oak Ridge National Laboratory"""


# __all__ = ()


from .install import Installer
from .match import Facet
from .database import (
    DictDatabase,
    RedisDatabase,
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
from .serializer import (
    PickleSerializer,
    StringSerializer,
    StringSJSerializer,
)
from .tokenizer import (
    SplitTokenizer,
    SpacyTokenizer,
)

# Defaults
Serializer = PickleSerializer
Database = DictDatabase
FeatureExtractor = CharacterFeatures
Similarity = CosineSimilarity
Tokenizer = SpacyTokenizer
