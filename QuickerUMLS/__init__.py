"""Facet package."""


__title__ = "QuickerUMLS"
__name__ = "QuickerUMLS"
__version__ = "0.8"
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


from .quickumls import QuickUMLS
from .database import (
    DictDatabase,
    RedisDatabase,
)
from .simstring import (
    DiceSimilarity,
    ExactSimilarity,
    CosineSimilarity,
    JaccardSimilarity,
    OverlapSimilarity,
    NgramExtractor,
)
from .serializer import PickleSerializer
from .simstring import simstring

# Defaults
Serializer = PickleSerializer
Databse = DictDatabase
