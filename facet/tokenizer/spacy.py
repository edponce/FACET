import spacy
from .base import BaseTokenizer
from typing import (
    Tuple,
    Union,
    Iterator,
)


__all__ = ['SpaCyTokenizer']


class SpaCyTokenizer(BaseTokenizer):
    """SpaCy-based tokenizer.

    Args:
        chunker (str): Phrase chunker where 'noun' uses nouns only,
            'noun_chunks' uses basic noun chunking, 'pos_chunks' uses
            parts-of-speech for chunking, None uses window-based
            tokenization. If chunking is enabled then 'window',
            'stopwords', and 'min_token_length' parameters are not used.

        lemmatizer (bool): If set use the lemma form of tokens.

        language (str): Language to use for processing corpora.

    Notes:
        * Stopwords includes boolean words. If affirmation/negation is
          desired, then these should be removed.
    """

    NAME = 'spacy'

    # For reference only, these are the universal POS tags.
    # https://spacy.io/api/annotation#pos-universal
    _UNIVERSAL_POS_TAGS = (
        'ADJ', 'ADP', 'ADV', 'AUX', 'CONJ', 'CCONJ', 'DET', 'INTJ',
        'NOUN', 'NUM', 'PART', 'PRON', 'PROPN', 'PUNCT', 'SCONJ',
        'SYM', 'VERB', 'X', 'SPACE',
    )

    def __init__(
        self,
        *,
        chunker: str = None,
        lemmatizer: bool = True,
        language: str = 'en',
        **kwargs,
    ):
        try:
            nlp = spacy.load(language)
        except KeyError as ex:
            raise ex(f"Model for spaCy language '{language}' is invalid.")
        except RuntimeError as ex:
            raise ex(f"Run 'python -m spacy download {language}'")

        # Set class's stop words, then initialize base class to allow
        # customization of stop words.
        type(self)._STOPWORDS = nlp.Defaults.stop_words
        super().__init__(**kwargs)

        # spacy.load('en') creates a model with 'tagger', 'parser', and 'NER',
        # where 'parser' performs sentence segmentation. We can also create a
        # model with empty pipeline via spacy.lang.en.English but this will
        # prevent using the 'language' parameter directly. Other languages
        # might not be preconfigured with the same components, so this may
        # fail. Solution is to check components existence via '*.pipeline'.
        # If chunking mode is enabled then dependency parser is not removed,
        # else we only need sentencizer and tagger (POS). The same pipeline
        # is used for 'sentencize()' --> 'tokenize()' and 'tokenize()'.
        nlp.remove_pipe('ner')
        if chunker is None:
            nlp.remove_pipe('parser')
            nlp.add_pipe(nlp.create_pipe('sentencizer'))
        self._nlp = nlp

        chunker_func_map = {
            'nouns': self._tokenize_with_nouns,
            'noun_chunks': self._tokenize_with_noun_chunks,
            'pos_chunks': self._tokenize_with_pos_chunks,
            # Let base class handle tokenization window.
            None: super().tokenize,
        }

        self._chunker = chunker_func_map[chunker]
        self._lemmatizer = lemmatizer
        self._language = language

    def _is_valid_token(self, token: 'spacy.tokens.token.Token'):
        return (
            len(token) >= self._min_token_length
            and token not in self._stopwords
        )

    def sentencize(
        self,
        text: str,
        as_tuple: bool = False,
    ) -> Iterator[Union[Tuple[int, int, str], 'spacy.tokens.span.Span']]:
        # spaCy uses an object-model for NLP, so to support invoking
        # 'sentencize()' directly we convert the objects to span-text tuples.
        if as_tuple:
            yield from (
                (
                    sentence[0].idx,
                    sentence[-1].idx + len(sentence[-1]) - 1,
                    # NOTE: spaCy lemmatizes sentences, so we take
                    # advantage of it.
                    self.convert(self._lemmatize(sentence)),
                )
                for sentence in self._sentencize(text)
            )
        else:
            # NOTE: Converters are applied to the input text because
            # we cannot modify spaCy objects and maintain consistency of
            # attributes.
            yield from self._sentencize(self.convert(text))

    def tokenize(
        self,
        text: Union[str, 'spacy.tokens.span.Span'],
    ) -> Iterator[Tuple[int, int, str]]:
        # NOTE: Support raw strings to allow invoking directly, that is,
        # it is not necessary to 'sentencize()' first.
        yield from self._chunker(
            self._nlp(text)
            if isinstance(text, str)
            else text
        )

    def _sentencize(self, text) -> Iterator['spacy.tokens.span.Span']:
        yield from self._nlp(text).sents

    def _lemmatize(self, text: 'spacy.tokens.token.Token') -> str:
        if self._lemmatizer:
            # NOTE: spaCy's lemma for some pronouns/determinants is
            # '-PRON-', so we skip these.
            lemma = text.lemma_
            return lemma if lemma not in ('-PRON-', '-pron-') else text.text
        else:
            return text.text

    def _tokenize(self, text: 'spacy.tokens.span.Span'):
        """Tokenizer with stop words."""
        yield from (
            (
                token.idx,
                token.idx + len(token) - 1,
                self._lemmatize(token),
            )
            for token in filter(self._is_valid_token, text)
        )

    def _tokenize_with_nouns(self, text: 'spacy.tokens.span.Span'):
        """Tokenizer for single nouns."""
        def is_valid_pos(token: 'spacy.tokens.token.Token'):
            return token.pos_ in ('NOUN', 'PROPN', 'X')

        yield from (
            (
                token.idx,
                token.idx + len(token) - 1,
                self._lemmatize(token),
            )
            for token in filter(
                self._is_valid_token,
                filter(is_valid_pos, text),
            )
        )

    def _tokenize_with_noun_chunks(self, text: 'spacy.tokens.span.Span'):
        """Tokenizer for noun chunks."""
        yield from (
            (
                chunk[0].idx,
                chunk[-1].idx + len(chunk[-1]) - 1,
                ' '.join(map(lambda t: self._lemmatize(t), chunk)),
            )
            for chunk in text.noun_chunks
            if len(chunk) > 1 or self._is_valid_token(chunk[0])
        )

    def _tokenize_with_pos_chunks(self, text: 'spacy.tokens.span.Span'):
        """Phrase tokenizer with parts-of-speech tags for marking bounds."""
        def is_valid_pos(token: 'spacy.tokens.token.Token'):
            return token.pos_ in (
                'ADJ', 'ADP', 'ADV', 'AUX', 'CONJ', 'DET', 'NOUN', 'PROPN',
                'PART', 'VERB', 'X',
            )

        def is_valid_begin_pos(token: 'spacy.tokens.token.Token'):
            return token.pos_ in (
                'ADJ', 'ADV', 'DET', 'NOUN', 'PROPN', 'VERB', 'X'
            )

        def is_valid_middle_pos(token: 'spacy.tokens.token.Token'):
            return token.pos_ in (
                'ADJ', 'ADP', 'ADV', 'AUX', 'CONJ', 'DET', 'NOUN', 'PROPN',
                'PART', 'VERB', 'X',
            )

        def is_valid_end_pos(token: 'spacy.tokens.token.Token'):
            return token.pos_ in ('NOUN', 'PROPN', 'VERB', 'X')

        chunk = []
        for token in filter(is_valid_pos, text):
            # Flag for not duplicating flush of a single token valid
            # as both, end and begin POS.
            is_end_token = False

            # Check for end token first:
            #   Handle single word tokens
            #   An end token can also be a begin token of another phrase
            if is_valid_end_pos(token):
                # NOTE: Split based on chunk size to improve performance.
                if len(chunk) == 0:
                    if self._is_valid_token(token):
                        is_end_token = True
                        yield (
                            token.idx,
                            token.idx + len(token) - 1,
                            self._lemmatize(token),
                        )
                else:
                    is_end_token = True
                    chunk.append(token)
                    yield (
                        chunk[0].idx,
                        token.idx + len(token) - 1,
                        ' '.join(map(lambda t: self._lemmatize(t), chunk)),
                    )
                    chunk = []

            if (
                is_valid_begin_pos(token)
                or (len(chunk) > 0 and is_valid_middle_pos(token))
            ):
                chunk.append(token)

        # Use remaining chunk span if not a single end token
        if len(chunk) > 0 and not is_end_token:
            yield (
                chunk[0].idx,
                chunk[-1].idx + len(chunk[-1]) - 1,
                ' '.join(map(lambda t: self._lemmatize(t), chunk)),
            )
            # chunk = []
