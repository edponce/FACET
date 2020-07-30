import nltk
from .base import BaseTokenizer
from typing import (
    Tuple,
    Iterator,
)


__all__ = ['NLTKTokenizer']


class NLTKTokenizer(BaseTokenizer):
    """NLTK-based Treebank tokenizer.

    Args:
        sentencizer (str): Name of sentencizer for text.

        chunker (str): Phrase chunker where 'noun' uses nouns only,
            'noun_chunks' uses basic noun chunking, 'pos_chunks' uses
            parts-of-speech for chunking, None uses window-based
            tokenization. If chunking is enabled then 'window',
            'stopwords', and 'min_token_length' parameters are not used.

        tokenizer (str): Name of tokenizer for sentences.

        lemmatizer (str): Name of lemmatizer for tokens. None = disabled.


        language (str): Language to use for processing corpora.
    """

    NAME = 'nltk'

    # For reference only, these are the universal POS tags.
    # https://spacy.io/api/annotation#pos-universal
    _UNIVERSAL_POS_TAGS = (
        'ADJ', 'ADP', 'ADV', 'AUX', 'CONJ', 'CCONJ', 'DET', 'INTJ',
        'NOUN', 'NUM', 'PART', 'PRON', 'PROPN', 'PUNCT', 'SCONJ',
        'SYM', 'VERB', 'X', 'SPACE',
    )

    _SENTENCIZER_MAP = {
        'line': nltk.tokenize.LineTokenizer,
        'punctuation': nltk.tokenize.PunktSentenceTokenizer,
    }

    _TOKENIZER_MAP = {
        # NOTE: The following tokenizers raise 'NotImplementedError'
        # for 'span_tokenize()'.
        # 'nltk': nltk.tokenize.NLTKWordTokenizer,
        # 'toktok': nltk.tokenize.ToktokTokenizer,
        'treebank': nltk.tokenize.TreebankWordTokenizer,
        # NOTE: Will be deprecated in v3.2.5, NLTK recommends
        # nltk.parse.corenlp.CoreNLPTokenizer, but this does not exists.
        # 'stanford': nltk.tokenize.StanfordSegmenter,
        'punctuation': nltk.tokenize.WordPunctTokenizer,
        'space': nltk.tokenize.SpaceTokenizer,
        'whitespace': nltk.tokenize.WhitespaceTokenizer,
    }

    _LEMMATIZER_MAP = {
        'ci': nltk.stem.Cistem,
        'isri': nltk.stem.ISRIStemmer,
        'lancaster': nltk.stem.LancasterStemmer,
        'porter': nltk.stem.PorterStemmer,
        'snowball': nltk.stem.SnowballStemmer,
        'rslps': nltk.stem.RSLPStemmer,
        'wordnet': nltk.stem.WordNetLemmatizer,

    }

    def __init__(
        self,
        *,
        sentencizer: str = 'punctuation',
        chunker: str = None,
        tokenizer: str = 'treebank',
        lemmatizer: str = 'snowball',
        language: str = 'english',
        **kwargs,
    ):
        # Set class's stop words, then initialize base class to allow
        # customization of stop words.
        try:
            type(self)._STOPWORDS = set(nltk.corpus.stopwords.words(language))
        except ValueError as ex:
            raise ex(f"Model for NLTK language '{language}' is invalid.")
        super().__init__(**kwargs)

        chunker_func_map = {
            'nouns': self._tokenize_with_nouns,
            'noun_chunks': self._tokenize_with_noun_chunks,
            'pos_chunks': self._tokenize_with_pos_chunks,
            # Let base class handle tokenization window.
            None: super().tokenize,
        }

        self._sentencizer = type(self)._SENTENCIZER_MAP[sentencizer]()
        self._chunker = chunker_func_map[chunker]
        self._tokenizer = type(self)._TOKENIZER_MAP[tokenizer]()
        # NOTE: Need to set 'language' before '_get_lemmatizer()'.
        self._language = language
        self._lemmatizer = self._get_lemmatizer(lemmatizer)
        self._parser = nltk.RegexpParser('NP: {<ADJ>*<NOUN>}')

    def _get_lemmatizer(self, lemmatizer: str):
        if lemmatizer is None:
            return lemmatizer
        # NOTE: This may trigger a LookupError if the stemmer/lemmatizer
        # resource is not found/installed.
        elif lemmatizer == 'snowball':
            _lemmatizer = (
                type(self)._LEMMATIZER_MAP[lemmatizer](self._language)
            )
        else:
            _lemmatizer = type(self)._LEMMATIZER_MAP[lemmatizer]()

        # NOTE: In NLTK, WordNetLemmatizer API differs from stemmers.
        if lemmatizer == 'wordnet':
            _lemmatizer.stem = _lemmatizer.lemmatize

        # NOTE: This may trigger a LookupError if the stemmer/lemmatizer
        # resource is not found/installed.
        _lemmatizer.stem('testing')
        return _lemmatizer

    def _pos_tag(
        self,
        text: Tuple[int, int, str],
    ) -> Iterator[Tuple[Tuple[int, int], Tuple[str, str]]]:
        """Parts-of-speech tagging."""
        spans = []
        tokens = []
        for begin, end in self._tokenizer.span_tokenize(text[2]):
            spans.append((text[0] + begin, text[0] + end - 1))
            tokens.append(text[2][begin:end])

        # NOTE: Language for nltk.pos_tag() is based on
        # ISO 639-2 (3 letter code). We take the first 3-letters of language
        # set for nltk.stopwords.words() although this is not always correct,
        # but we chose this approach for simplicity.
        yield from zip(
            spans,
            nltk.pos_tag(tokens, tagset='universal', lang=self._language[:3])
        )

    def _is_valid_token(self, token: str):
        return (
            len(token) >= self._min_token_length
            and token not in self._stopwords
        )

    def tokenize(self, text):
        # NOTE: Support raw strings to allow invoking directly, that is,
        # it is not necessary to 'sentencize()' first.
        yield from self._chunker(
            (0, len(text) - 1, text)
            if isinstance(text, str)
            else text
        )

    def _sentencize(self, text):
        yield from (
            (begin, end - 1, text[begin:end])
            for begin, end in self._sentencizer.span_tokenize(text)
        )

    def _lemmatize(self, text: str) -> str:
        return (
            text
            if self._lemmatizer is None
            else self._lemmatizer.stem(text)
        )

    def _tokenize(self, text: Tuple[int, int, str]):
        sentence = text[2]
        # for begin, end in self._tokenizer.span_tokenize(sentence):
        #     token = sentence[begin:end]
        #     if self._is_valid_token(token):
        #         yield (
        #             text[0] + begin,
        #             text[0] + end - 1,
        #             self._lemmatize(token),
        #         )
        yield from (
            (
                text[0] + begin,
                text[0] + end - 1,
                self._lemmatize(sentence[begin:end]),
            )
            for begin, end in self._tokenizer.span_tokenize(sentence)
            if self._is_valid_token(sentence[begin:end])
        )

    def _tokenize_with_nouns(self, text: Tuple[int, int, str]):
        """Tokenizer for single nouns."""
        def is_valid_pos(pos: str):
            return pos in ('NOUN', 'PROPN', 'X')

        yield from (
            (*span, self._lemmatize(token))
            for span, (token, pos) in self._pos_tag(text)
            if is_valid_pos(pos) and self._is_valid_token(token)
        )

    def _tokenize_with_noun_chunks(self, text: Tuple[int, int, str]):
        """Tokenizer for noun chunks."""
        def is_valid_pos(node: 'nltk.tree.Tree'):
            return isinstance(node, nltk.tree.Tree) and node.label() == 'NP'

        # Parser requires tags in an iterable, so we unpack them.
        spans, tags = zip(*self._pos_tag(text))

        # NOTE: Traverse parser tree assuming it has height = 3.
        spans = iter(spans)
        for node in self._parser.parse(tags):
            span = next(spans)
            if is_valid_pos(node):
                begin = span[0]
                for _ in range(len(node) - 1):
                    span = next(spans)
                yield (
                    begin,
                    span[1],
                    ' '.join(map(lambda t: self._lemmatize(t[0]), node)),
                )

    def _tokenize_with_pos_chunks(self, text: Tuple[int, int, str]):
        """Phrase tokenizer with parts-of-speech tags for marking bounds."""
        def is_valid_pos(pos: str):
            return pos in (
                'ADJ', 'ADP', 'ADV', 'AUX', 'CONJ', 'DET', 'NOUN', 'PROPN',
                'PART', 'VERB', 'X',
            )

        def is_valid_begin_pos(pos: str):
            return pos in ('ADJ', 'ADV', 'DET', 'NOUN', 'PROPN', 'VERB', 'X')

        def is_valid_middle_pos(pos: str):
            return pos in (
                'ADJ', 'ADP', 'ADV', 'AUX', 'CONJ', 'DET', 'NOUN', 'PROPN',
                'PART', 'VERB', 'X',
            )

        def is_valid_end_pos(pos: str):
            return pos in ('NOUN', 'PROPN', 'VERB', 'X')

        spans = []
        tokens = []
        for span, (token, pos) in self._pos_tag(text):
            if not is_valid_pos(pos):
                continue

            # Flag for not duplicating flush of a single token valid
            # as both, end and begin POS.
            is_end_token = False

            # Check for end token first:
            #   Handle single word tokens
            #   An end token can also be a begin token of another phrase
            if is_valid_end_pos(pos):
                # NOTE: Split based on chunk size to improve performance.
                if len(spans) == 0:
                    if self._is_valid_token(token):
                        is_end_token = True
                        yield (*span, self._lemmatize(token))
                else:
                    is_end_token = True
                    tokens.append(token)
                    yield (
                        spans[0][0],
                        span[1],
                        ' '.join(map(lambda t: self._lemmatize(t), tokens)),
                    )
                    spans = []
                    tokens = []

            if (
                is_valid_begin_pos(pos)
                or (len(tokens) > 0 and is_valid_middle_pos(pos))
            ):
                spans.append(span)
                tokens.append(token)

        # Use remaining chunk span if not a single end token
        if len(spans) > 0 and not is_end_token:
            yield (
                spans[0][0],
                spans[-1][1],
                ' '.join(map(lambda t: self._lemmatize(t), tokens)),
            )
            # spans = []
            # tokens = []
