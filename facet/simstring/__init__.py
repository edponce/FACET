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
from .base import BaseSimstring
from .simstring import Simstring
from .elasticsearch import ElasticsearchSimstring


simstring_map = {
    'simstring': Simstring,
    'elasticsearch': ElasticsearchSimstring,
}
