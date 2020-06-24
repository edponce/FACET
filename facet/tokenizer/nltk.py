import nltk
from .base import BaseTokenizer


__all__ = ['NLTKTokenizer']


class NLTKTokenizer(BaseTokenizer):
    """NLTK-based Treebank tokenizer.

    Args:
        language (str): Language to use for processing corpora.
            Default is 'english'.
    """

    _CONTRACTIONS_MAP = {
        "n't": 'not',
        "'ll": 'will',  # [shall, will]
        "'ve": 'have',
        "'d": 'would',  # [had, would]
        "'s": 'is',     # [is, has], special case: let's = let us
        "'re": 'are',   # [are, were]
    }

    def __init__(self, *, language: str = 'english'):
        try:
            self._stopwords = set(nltk.corpus.stopwords.words(language))
        except OSError:
            err = f"Model for language '{language}' is not valid."
            raise OSError(err)
        self._sentencizer = nltk.tokenize.PunktSentenceTokenizer()
        self._tokenizer = nltk.tokenize.TreebankWordTokenizer()
        self._language = language

    def sentencize(self, text):
        for sentence in self._sentencizer.tokenize(text):
            yield sentence

    def _tokenize(self, text):
        for begin, end in self._tokenizer.span_tokenize(text):
            token = text[begin:end]
            if token in type(self)._CONTRACTIONS_MAP:
                token = type(self)._CONTRACTIONS_MAP[token]
            if token not in self._stopwords:
                yield (begin, end, token)

    def tokenize(self, text):
        """Use parts-of-speech tagger to filter tokens."""
        spans = []
        tokens = []
        for begin, end, token in self._tokenize(text):
            spans.append((begin, end - 1))
            tokens.append(token)

        # NOTE: Language for nltk.pos_tag() is based on
        # ISO 639-2 (3 letter code). We take the first 3-letters of language
        # set for nltk.stopwords.words() although this is not always correct,
        # but we chose this approach for simplicity.
        for span, token_pos in zip(spans,
                                   nltk.pos_tag(tokens,
                                                tagset='universal',
                                                lang=self._language[:3])):
            token, pos = token_pos
            if self._is_valid_token(pos):
                yield (*span, token)

    def _is_valid_token(self, pos) -> bool:
        return pos in {'NOUN', 'VERB', 'ADV', 'ADJ'}