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
from .mongo_simstring import MongoSimstring
from .redisearch_simstring import RediSearchSimstring
from .elasticsearch_simstring import ElasticsearchSimstring
from .elasticsearch_fuzzy import ElasticsearchFuzzy


matcher_map = {
    'simstring': Simstring,
    'mongo-simstring': MongoSimstring,
    'redisearch-simstring': RediSearchSimstring,
    'elasticsearch-simstring': ElasticsearchSimstring,
    'elasticsearch-fuzzy': ElasticsearchFuzzy,
}
