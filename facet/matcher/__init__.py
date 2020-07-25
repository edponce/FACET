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
from .mongo import MongoSimstring
from .redisearch import (
    RediSearch,
    RediSearchSimstring,
    RediSearchAutoCompleter,
)
from .elasticsearch import (
    ElasticsearchFuzzy,
    ElasticsearchSimstring,
)
from typing import Union


matcher_map = {
    Simstring.NAME: Simstring,
    MongoSimstring.NAME: MongoSimstring,
    RediSearch.NAME: RediSearch,
    RediSearchSimstring.NAME: RediSearchSimstring,
    RediSearchAutoCompleter.NAME: RediSearchAutoCompleter,
    ElasticsearchFuzzy.NAME: ElasticsearchFuzzy,
    ElasticsearchSimstring.NAME: ElasticsearchSimstring,
}


def get_matcher(value: Union[str, 'BaseMatcher']):
    if isinstance(value, str):
        return matcher_map[value]()
    elif isinstance(value, BaseMatcher):
        return value
    raise ValueError(f'invalid matcher, {value}')
