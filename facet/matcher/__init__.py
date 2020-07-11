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
from .base_simstring import BaseSimstring
from .simstring import Simstring
from .elasticsearch_simstring import ElasticsearchSimstring


matcher_map = {
    'simstring': Simstring,
    'elasticsearch-simstring': ElasticsearchSimstring,
}
