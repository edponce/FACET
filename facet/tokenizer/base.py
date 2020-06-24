from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Set,
    Tuple,
    Iterator,
)


__all__ = ['BaseTokenizer']


class BaseTokenizer(ABC):
    """Class supporting sentence segmentation and tokenization."""

    def __init__(self):
        self._stopwords = set()

    def __call__(self, text: str):
        for sentence in self.sentencize(text):
            for token in self.tokenize(sentence):
                yield token

    @property
    def stopwords(self) -> Set[str]:
        return self._stopwords

    @stopwords.setter
    def stopwords(self, stopwords: Set[str]):
        self._stopwords = stopwords

    @abstractmethod
    def sentencize(self, text: str) -> Iterator[str]:
        pass

    @abstractmethod
    def tokenize(self, text: str) -> Iterator[Tuple[int, int, str]]:
        pass