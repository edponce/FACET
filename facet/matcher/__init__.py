from .similarity import (
    DiceSimilarity,
    ExactSimilarity,
    CosineSimilarity,
    JaccardSimilarity,
    OverlapSimilarity,
    HammingSimilarity,
)
from .ngram import (
    WordNgram,
    CharacterNgram,
)
from .base import BaseMatcher
from .simstring import Simstring
from .elasticsearch import ElasticsearchSimstring


matcher_map = {
    'simstring': Simstring,
    'elasticsearch': ElasticsearchSimstring,
}
