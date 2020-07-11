from .base import BaseDatabase
from .dict import DictDatabase
from .redis import RedisDatabase
from .sqlite import SQLiteDatabase
from .elasticsearch import ElasticsearchDatabase, ElasticsearchKVDatabase


database_map = {
    'dict': DictDatabase,
    'redis': RedisDatabase,
    'sqlite': SQLiteDatabase,
    'elasticsearch': ElasticsearchDatabase,
}
