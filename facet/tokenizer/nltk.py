import nltk
from .base import BaseTokenizer


__all__ = ['NLTKTokenizer']


class NLTKTokenizer(BaseTokenizer):
    """NLTK-based Treebank tokenizer.

    Args:
        language (str): Language to use for processing corpora.
            Default is 'english'.
    """

    def __init__(self, *, language: str = 'english', **kwargs):
        super().__init__(**kwargs)

        try:
            self.STOPWORDS = set(nltk.corpus.stopwords.words(language))
        except ValueError:
            err = f"Model for language '{language}' is not valid."
            raise ValueError(err)
        self._sentencizer = nltk.tokenize.PunktSentenceTokenizer()
        self._tokenizer = nltk.tokenize.TreebankWordTokenizer()
        self._language = language

    def sentencize(self, text):
        for sentence in self._sentencizer.tokenize(text):
            yield sentence

    def _tokenize(self, text):
        for begin, end in self._tokenizer.span_tokenize(text):
            token = text[begin:end]
            # NOTE: If contractions are stopwords, then do one check only.
            token = type(self)._CONTRACTIONS_MAP.get(token, token)
            if len(token) > 1 and token not in self.STOPWORDS:
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
        for span, token_pos in zip(
            spans,
            nltk.pos_tag(tokens, tagset='universal', lang=self._language[:3])
        ):
            token, pos = token_pos
            if self._is_valid_token(pos):
                yield (*span, token)

    def _is_valid_token(self, pos) -> bool:
        return pos in {'NOUN', 'VERB', 'ADV', 'ADJ'}
