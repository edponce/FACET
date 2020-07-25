from .base import BaseDatabase
from .dict import DictDatabase
from .redis import (
    RedisDatabase,
    RediSearchDatabase,
    RediSearchAutoCompleterDatabase,
)
from .mongo import MongoDatabase
from .sqlite import SQLiteDatabase
from .elasticsearch import ElasticsearchDatabase
from typing import Union


database_map = {
    # 'DictDatabase' is a factory class
    DictDatabase.NAME: DictDatabase(),
    RedisDatabase.NAME: RedisDatabase,
    RediSearchDatabase.NAME: RediSearchDatabase,
    RediSearchAutoCompleterDatabase.NAME: RediSearchAutoCompleterDatabase,
    MongoDatabase.NAME: MongoDatabase,
    SQLiteDatabase.NAME: SQLiteDatabase,
    ElasticsearchDatabase.NAME: ElasticsearchDatabase,
}


def get_database(value: Union[str, 'BaseDatabase']):
    if isinstance(value, str):
        return database_map[value]()
    elif value is None or isinstance(value, BaseDatabase):
        return value
    raise ValueError(f'invalid database, {value}')
