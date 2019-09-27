import time
import spacy
from .base import BaseTokenizer
from typing import Set, Tuple, Generator


__all__ = ['SpacyTokenizer']


class SpacyTokenizer(BaseTokenizer):
    """SpaCy-based tokenizer.

    Args:
        language (str): spaCy language to use for processing corpora.
            Default is 'en'.
    """

    def __init__(self, *, language: 'str' = 'en'):
        try:
            self._nlp = spacy.load(language)
        except KeyError:
            err = (f"Model for language '{language}' is not valid.")
            raise KeyError(err)
        except OSError:
            err = (f"Please run 'python3 -m spacy download {language}'")
            raise OSError(err)

    @property
    def stopwords(self) -> Set[str]:
        return self._nlp.Defaults.stop_words

    @stopwords.setter
    def stopwords(self, stopwords: Set[str]):
        self._nlp.Defaults.stop_words = stopwords

    def sentencize(self, text) -> Generator['spacy.Span', None, None]:
        # Creates a spaCy Doc object
        # len(Doc) == number of words
        t1 = time.time()
        doc = self._nlp(text)
        t2 = time.time()
        print(f'Num words: {len(doc)}')
        print(f'spaCy parse: {t2 - t1} s')
        return doc.sents

    def tokenize(
        self,
        text: 'spacy.Span',
        *,
        window: int = 5,
        min_match_length: int = 3,
        ignore_syntax: bool = False,
    ) -> Generator[Tuple[int, int, str], None, None]:
        """
        Args:
            sentence (spacy.Span): Sentence to process.

            window (int): Window size for processing words.
                Default value is 5.

            min_match_length (int): Minimum number of characters for matching
                criterion. Default value is 3.

            ignore_syntax (bool): Ignore token types when parsing text.
                Default is False.

        Returns (Generator[Tuple[int, int, str]]: Span start, span end,
            and text.
        """
        if ignore_syntax:
            tokens = self._tokenize(text, window, min_match_length)
        else:
            tokens = self._tokenize_syntax(text, window, min_match_length)
        return tokens

    def _tokenize(self, sentence, window, min_match_length):
        for i in range(len(sentence)):
            for j in range(i + 1, min(i + window, len(sentence)) + 1):
                span = sentence[i:j]
                if span.end_char - span.start_char >= min_match_length:
                    yield (span.start_char, span.end_char, span.text)

    def _tokenize_syntax(self, sentence, window, min_match_length):
        for i, token in enumerate(sentence):
            if not self._is_valid_token(token):
                continue

            # we take a shortcut if the token is the last one
            # in the sentence
            if (
                i + 1 == len(sentence)
                and len(token) >= min_match_length
                and self._is_valid_end_token(token)
            ):
                yield (token.idx, token.idx + len(token), token.text)
            else:
                span_start = i + 1 + int(not self._is_valid_begin_token(token))
                span_end = min(len(sentence), i + window) + 1
                for j in range(span_start, span_end):
                    if not self._is_valid_middle_token(sentence[j - 1]):
                        break

                    if not self._is_valid_end_token(sentence[j - 1]):
                        continue

                    span = sentence[i:j]

                    if span.end_char - span.start_char < min_match_length:
                        continue

                    yield (
                        span.start_char, span.end_char,
                        ''.join(
                            token.text_with_ws
                            for token in span
                            if token.pos_ != 'DET'
                        ).strip()
                    )

    def _is_valid_token(self, token) -> bool:
        return not(
            token.is_punct
            or token.is_space
            or token.pos_ in ('ADP', 'DET', 'CONJ')
        )

    def _is_valid_begin_token(self, token) -> bool:
        return not(
            token.like_num
            or token.pos_ in ('ADP', 'DET', 'CONJ')
            or token.text in self._nlp.Defaults.stop_words
        )

    def _is_valid_middle_token(self, token) -> bool:
        return (
            not(token.is_punct or token.is_space)
            or token.is_bracket
        )

    def _is_valid_end_token(self, token) -> bool:
        return not(
            token.is_punct
            or token.is_space
            or token.text in self._nlp.Defaults.stop_words
            or token.pos_ in ('ADP', 'DET', 'CONJ')
        )
