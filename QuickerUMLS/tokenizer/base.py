from typing import Tuple, Iterable
from abc import ABC, abstractmethod


__all__ = ['BaseTokenizer']


class BaseTokenizer(ABC):
    """Class supporting sentence segmentation and tokenization."""

    STOPWORDS = {
        "me", "my", "myself", "we", "our", "ours", "ourselves", "you",
        "you're", "you've", "you'll", "you'd", "your", "yours", "yourself",
        "yourselves", "he", "him", "his", "himself", "she", "she's", "her",
        "hers", "herself", "it", "it's", "its", "itself", "they", "them",
        "their", "theirs", "themselves", "what", "which", "who", "whom",
        "this", "that", "that'll", "these", "those", "am", "is", "are", "was",
        "were", "be", "been", "being", "have", "has", "had", "having", "do",
        "does", "did", "doing", "an", "the", "and", "but", "if", "or",
        "because", "as", "until", "while", "of", "at", "by", "for", "with",
        "about", "against", "between", "into", "through", "during", "before",
        "after", "above", "below", "to", "from", "up", "down", "in", "out",
        "on", "off", "over", "under", "again", "further", "then", "once",
        "here", "there", "when", "where", "why", "how", "all", "any", "both",
        "each", "few", "more", "most", "other", "some", "such", "no", "nor",
        "not", "only", "own", "same", "so", "than", "too", "very", "can",
        "will", "just", "don", "don't", "should", "should've", "now", "ll",
        "re", "ve", "ain", "aren", "aren't", "couldn", "couldn't", "didn",
        "didn't", "doesn", "doesn't", "hadn", "hadn't", "hasn", "hasn't",
        "haven", "haven't", "isn", "isn't", "ma", "mightn", "mightn't",
        "mustn", "mustn't", "needn", "needn't", "shan", "shan't", "shouldn",
        "shouldn't", "wasn", "wasn't", "weren", "weren't", "won", "won't",
        "wouldn", "wouldn't",
    }

    def __init__(self):
        self._stopwords = type(self).STOPWORDS

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
