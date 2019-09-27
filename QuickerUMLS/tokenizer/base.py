from abc import ABC, abstractmethod
from typing import Tuple, Set, Generator


__all__ = ['BaseTokenizer']


class BaseTokenizer(ABC):
    """Class supporting sentence segmentation and tokenization."""

    def __init__(self):
        self._stopwords = set()

    def __call__(self, text: str):
        for sentence in self.sentencize(text):
            return self.tokenize(sentence)

    @property
    def stopwords(self) -> Set[str]:
        return self._stopwords

    @stopwords.setter
    def stopwords(self, stopwords: Set[str]):
        self._stopwords.update(stopwords)

    @abstractmethod
    def sentencize(self, text: str) -> Generator[str, None, None]:
        pass

    @abstractmethod
    def tokenize(
        self,
        text: str,
    ) -> Generator[Tuple[int, int, str], None, None]:
        pass
