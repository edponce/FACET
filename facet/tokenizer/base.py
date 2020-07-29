import copy
from unidecode import unidecode
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Tuple,
    Union,
    Iterator,
    Iterable,
    Callable,
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

    _CONVERTERS_MAP = {
        'unidecode': unidecode,
        'lower': str.lower,
        'upper': str.upper,
    }

    def __init__(
        self,
        *,
        window: int = 1,
        min_token_length: int = 2,
        # NOTE: If token is not lowercased, it will not match stopwords,
        # so default converters include lowercasing. Converters are applied
        # at the sentence level for performance reasons.
        converters: Union[str, Iterable[Callable]] = ('unidecode', 'lower'),
        use_stopwords: bool = True,
        stopwords: Iterable[str] = None,
    ):
        self._window = window
        self._min_token_length = min_token_length

        self._stopwords = set()
        if use_stopwords:
            self._stopwords = (
                copy.deepcopy(type(self)._STOPWORDS)
                if stopwords is None
                else stopwords
            )

        # Make converters iterable and resolve strings to functions
        self._converters = []
        if converters is not None:
            if isinstance(converters, str):
                converters = [type(self)._CONVERTERS_MAP[converters]]
            elif callable(converters):
                converters = [converters]
            for converter in converters:
                if isinstance(converter, str):
                    self._converters.append(
                        type(self)._CONVERTERS_MAP[converter]
                    )
                elif callable(converter):
                    self._converters.append(converter)

    @property
    def stopwords(self):
        return self._stopwords

    def __call__(self, text: str) -> Iterator[str]:
        return (
            token
            for sentence in self.sentencize(text)
            for token in self.tokenize(sentence)
        )

    def convert(self, text):
        for converter in self._converters:
            text = converter(text)
        return text

    def sentencize(self, text: str) -> Iterator[Tuple[int, int, str]]:
        if len(self._converters) == 0:
            yield from self._sentencize(text)
        else:
            for begin, end, sentence in self._sentencize(text):
                yield begin, end, self.convert(sentence)

    def tokenize(
        self,
        text: Union[str, Tuple[int, int, str]],
    ) -> Iterator[Tuple[int, int, str]]:
        # NOTE: Support raw strings to allow invoking directly, that is,
        # it is not necessary to 'sentencize()' first.
        if isinstance(text, str):
            text = (0, len(text) - 1, text)

        if self._window == 1:
            yield from self._tokenize(text)
        else:
            tokens = list(self._tokenize(text))
            for i in range(len(tokens)):
                for j in range(i + 1, min(i + self._window,
                                          len(tokens)) + 1):
                    span = tokens[i:j]
                    yield (
                        span[0][0],
                        span[-1][1],
                        ' '.join(map(lambda token: token[2], span)),
                    )

    @abstractmethod
    def _sentencize(self, text: str) -> Iterator[Tuple[int, int, str]]:
        """
        Returns: span begin, span end, text.
        """
        pass

    @abstractmethod
    def _tokenize(
        self,
        text: Union[str, Tuple[int, int, str]],
    ) -> Iterator[Tuple[int, int, str]]:
        """
        Returns: span begin, span end, text.
        """
        pass
