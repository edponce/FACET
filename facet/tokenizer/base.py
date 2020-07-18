from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Tuple,
    Iterator,
)


__all__ = ['BaseTokenizer']


class BaseTokenizer(ABC):
    """Class supporting sentence segmentation and tokenization."""

    # Stopwords from spaCy
    _STOPWORDS = {
        'yours', 'do', 'might', 'although', 'all', 'he', "'s", 'wherein',
        'themselves', 'used', 'anyone', 'others', 'whom', 'off', 'doing',
        'she', 'no', 'even', 'behind', 'them', 'done', 'none', 'beside',
        'whither', 'yet', 'but', 'perhaps', 'one', 'its', 'thereby', 'on',
        'hereby', 'nine', 'just', 'ca', 'front', "'d", 'also', 'across',
        'seems', 'towards', 'together', 'itself', 'four', 'that', 'why', 'it',
        'so', 'three', 'down', 'keep', 'sixty', 'empty', 'above', 'whose',
        'between', 'hence', 'two', 'be', 'however', 'nothing', 'are', 'whence',
        're', 'beyond', 'being', 'nowhere', 'always', 'was', 'via', 'mine',
        'ourselves', 'quite', 'when', 'only', 'should', 'amongst', 'before',
        'from', 'any', 'most', 'how', 'same', 'if', 'latter', 'something',
        'fifty', 'an', 'have', 'who', 'too', 'up', 'ours', 'which',
        'beforehand', 'else', 'sometimes', 'to', 'either', 'really', 'take',
        'such', 'go', 'again', 'into', 'a', 'per', 'anyhow', 'anything',
        'elsewhere', 'hundred', 'afterwards', 'we', 'since', 'yourselves',
        'both', 'top', 'formerly', "'m", 'her', 'alone', 'whereas', 'becoming',
        'what', "n't", 'back', 'or', "'ll", 'never', 'everyone', 'various',
        'then', 'over', 'against', 'twelve', 'herself', 'those', 'they',
        'became', 'thereafter', 'forty', 'latterly', 'seeming', 'see', 'of',
        'much', 'thereupon', 'regarding', 'get', 'give', 'by', 'indeed', "'ve",
        'thus', 'part', 'can', 'still', 'is', 'nor', 'first', 'eight',
        'nevertheless', 'another', 'along', 'i', 'former', 'someone',
        'without', 'noone', 'whoever', 'becomes', 'about', 'through', 'unless',
        'fifteen', 'namely', 'anywhere', 'will', 'as', 'each', 'during', 'few',
        'become', 'their', 'hereafter', 'could', 'third', 'thru', 'somehow',
        'in', 'bottom', 'am', 'seem', 'otherwise', 'here', 'several', 'say',
        'would', 'our', 'for', 'due', 'move', 'somewhere', 'under', 'himself',
        'already', 'with', 'except', 'mostly', 'amount', 'more', 'you', 'his',
        'almost', 'every', 'upon', 'throughout', 'often', 'below', 'been',
        'whatever', 'eleven', 'whole', 'within', 'cannot', 'five', 'him',
        'hers', 'yourself', 'next', 'once', 'around', "'re", 'thence', 'using',
        'does', 'until', 'were', 'make', 'onto', 'us', 'your', 'while',
        'because', 'some', 'show', 'full', 'everything', 'did', 'after',
        'call', 'now', 'the', 'meanwhile', 'many', 'whereupon', 'everywhere',
        'me', 'name', 'not', 'seemed', 'least', 'must', 'six', 'less',
        'serious', 'there', 'whereby', 'whether', 'own', 'and', 'whereafter',
        'has', 'may', 'neither', 'where', 'ever', 'wherever', 'made',
        'moreover', 'well', 'myself', 'among', 'please', 'other', 'out',
        'this', 'therein', 'rather', 'though', 'hereupon', 'besides', 'had',
        'at', 'twenty', 'ten', 'these', 'my', 'than', 'side', 'nobody', 'very',
        'last', 'sometime', 'toward', 'herein', 'whenever', 'further',
        'anyway', 'enough', 'therefore', 'put',
    }

    _CONTRACTIONS_MAP = {
        "n't": 'not',
        "'ll": 'will',  # [shall, will]
        "'ve": 'have',
        "'d": 'would',  # [had, would]
        "'s": 'is',     # [is, has], special case: let's = let us
        "'re": 'are',   # [are, were]
    }

    def __init__(self):
        # NOTE: We should add options to enable/disable sentence segmentation
        # and tokenization.
        self.STOPWORDS = type(self)._STOPWORDS

    def __call__(self, text: str):
        for sentence in self.sentencize(text):
            for token in self.tokenize(sentence):
                yield token

    @abstractmethod
    def sentencize(self, text: str) -> Iterator[str]:
        pass

    @abstractmethod
    def tokenize(self, text: str) -> Iterator[Tuple[int, int, str]]:
        pass
