from .base import BaseNgram
from .word import WordNgram
from .character import CharacterNgram
from typing import Union


ngram_map = {
    WordNgram.NAME: WordNgram,
    CharacterNgram.NAME: CharacterNgram,
}


def get_ngram(value: Union[str, 'BaseNgram']):
    if isinstance(value, str):
        return ngram_map[value]()
    elif isinstance(value, BaseNgram):
        return value
    raise ValueError(f'invalid n-gram feature extractor, {value}')
