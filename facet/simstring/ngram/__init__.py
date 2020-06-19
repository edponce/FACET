from .base import BaseNgram
from .word import WordNgram
from .character import CharacterNgram


ngram_map = {
    'word': WordNgram,
    'character': CharacterNgram,
}
