from .base import BaseDatabase
from .dict import DictDatabase
from .redis import RedisDatabase
from .mongo import MongoDatabase
from .sqlite import SQLiteDatabase
from .elasticsearch import ElasticsearchDatabase


database_map = {
    'dict': DictDatabase,
    'redis': RedisDatabase,
    'mongo': MongoDatabase,
    'sqlite': SQLiteDatabase,
    'elasticsearch': ElasticsearchDatabase,
}
