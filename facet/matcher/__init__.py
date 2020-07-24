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
from typing import Union


matcher_map = {
    Simstring.NAME: Simstring,
    MongoSimstring.NAME: MongoSimstring,
    RediSearchSimstring.NAME: RediSearchSimstring,
    ElasticsearchSimstring.NAME: ElasticsearchSimstring,
    ElasticsearchFuzzy.NAME: ElasticsearchFuzzy,
}


def get_matcher(value: Union[str, 'BaseMatcher']):
    if isinstance(value, str):
        return matcher_map[value]()
    elif isinstance(value, BaseMatcher):
        return value
    raise ValueError(f'invalid matcher, {value}')
