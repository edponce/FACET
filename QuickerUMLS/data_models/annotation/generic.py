import os
from typing import Any, Dict, List, Optional
from factory.namedtuple import namedtupleFactory


_AnnotationBase = namedtupleFactory(
    'AnnotationBase',
    ('category',
     'begin',
     'end',
     'ngram')
)


class Annotation(_AnnotationBase):
    '''A text annotation.

    Args:
        begin (int): Index of first character.

        end (int): Index of one character after last character. This creates
            a non-inclusive end span (similar to Python indexing scheme).

        kwargs (Dict[str, Any], Optional): Map of attributes and values.


    An :class:`Annotation` represents the text span associated with a label.
    The text spans a number of characters and this is represented
    with the index of its starting and non-inclusive ending character. Span is
    [:attr:`begin`,:attr:`end`). A :class:`Annotation` object is immutable.

    Attributes:
        begin (int): Index of first character in text span.

        end (int): Index of one character after last character in text span.
            Index is non-inclusive (similar to Python indexing scheme).

        ngram (str, Optional): Text data represented by the span indices.

        category (Any, Optional): Label/tag of the text span.

    Note:

        * The :attr:`category` attribute might be removed because container
          already handles annotation categories. Nevertheless, it may be useful
          for when dealing with individual annotations.
    '''

    def __new__(cls, begin: int, end: int, **kwargs: Optional[Dict[str, Any]]):
        # Validate span
        # Subtract 1 because it is non-inclusive end
        if begin > end - 1 or begin < 0 or end < 0:
            raise ValueError(f'invalid span indices [{begin},{end})')
        # Validate text is consistent with span
        ngram = kwargs.get('ngram')
        if ngram is not None and len(ngram) != (end - begin):
            raise ValueError(f'invalid ngram/span values')
        kwargs['begin'] = begin
        kwargs['end'] = end
        return super().__new__(cls, **kwargs)

    def __get_attributes_list(self) -> List:
        ss = []
        for f in self._fields:
            v = getattr(self, f)
            if isinstance(v, str):
                ss.append(f"{f}='{v}'")
            else:
                ss.append(f'{f}={v}')
        return ss

    def __repr__(self) -> str:
        return f'{type(self).__qualname__}' + '(' + ', '.join(self.__get_attributes_list()) + ')'

    def __str__(self) -> str:
        return os.linesep.join(self.__get_attributes_list())

    def __len__(self) -> int:
        return self.end - self.begin

    def copy(self) -> 'Annotation':
        return type(self)(**self._asdict())
