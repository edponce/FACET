from typing import Tuple, Iterable
from abc import ABC, abstractmethod


__all__ = ['BaseTokenizer']


class BaseTokenizer(ABC):
    """Class supporting sentence segmentation and tokenization."""

    def __init__(self):
        self._stopwords = set()

    @property
    def stopwords(self) -> Iterable[str]:
        return self._stopwords

    @stopwords.setter
    def stopwords(self, stopwords: Iterable[str]):
        self._stopwords.update(stopwords)

    @abstractmethod
    def sentencize(self, text: str) -> Iterable[str]:
        pass

    @abstractmethod
    def tokenize(self, text: str) -> Iterable[Tuple[int, int, str]]:
        pass
