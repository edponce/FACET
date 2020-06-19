from .base import BaseDatabase
from .dict import DictDatabase
from .redis import RedisDatabase
from .elasticsearchx import Elasticsearchx, ElasticsearchDatabase


database_map = {
    'dict': DictDatabase,
    'redis': RedisDatabase,
    'elasticsearch': ElasticsearchDatabase,
}
