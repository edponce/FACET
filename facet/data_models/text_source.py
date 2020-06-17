import os
from typing import List
from faker import Faker


class TextSource:
    def __init__(self, provider='lorem', seed=4321):
        self.fake = Faker()
        self.fake.add_provider(provider)
        self.fake.seed_instance(seed)

    def file(self, ext='txt') -> str:
        return self.fake.file_name(category='text', extension=ext)

    def uid(self) -> str:
        """Uses an employer identification number as a unique identifier."""
        return self.fake.ein()

    def did(self) -> str:
        """Uses an employer identification number as a document identifier."""
        return self.fake.ein()

    def text(self, num_chars=500) -> str:
        return self.fake.text(max_nb_chars=num_chars)

    def paragraph(self, num_sentences=5) -> str:
        return self.fake.paragraph(nb_sentences=num_sentences)

    def sentence(self, num_words=10) -> str:
        return self.fake.sentence(nb_words=num_words)

    def word(self) -> str:
        return self.fake.word()

    def punctuation(self) -> str:
        symbols = ('.', '!', ',', '?', ';', ':')
        return self.random_element(symbols)

    def whitespace(self) -> str:
        symbols = (' ', '\t', os.linesep)
        return self.random_element(symbols)

    def paragraphs(self, count=5) -> List[str]:
        return self.fake.paragraphs(nb=count)

    def sentences(self, count=5) -> List[str]:
        return self.fake.sentences(nb=count)

    def words(self, count=5) -> List[str]:
        return self.fake.words(nb=count, unique=True)
