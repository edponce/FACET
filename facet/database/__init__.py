from .base import BaseDatabase
from .base2 import BaseDatabase2
from .dict import DictDatabase
from .redis import RedisDatabase
from .sqlite import SQLiteDatabase
from .elasticsearchx import Elasticsearchx, ElasticsearchDatabase

# from .dict2 import DictDatabase

database_map = {
    'dict': DictDatabase,
    'redis': RedisDatabase,
    'sqlite': SQLiteDatabase,
    'elasticsearch': ElasticsearchDatabase,
}
