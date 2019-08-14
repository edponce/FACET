import os
from typing import Any, Dict, List, Optional
from factory.namedtuple import namedtupleFactory


_UMLSAnnotationBase = namedtupleFactory(
    'UMLSAnnotationBase',
    ('category',
     'begin',
     'end',
     'ngram',
     'term',
     'cui',
     'similarity',
     'semantic_types',
     'preferred')
)


class UMLSAnnotation(_UMLSAnnotationBase):
    '''A text annotation with UMLS-related attributes.

    Args:
        kwargs (Dict[str, Any], Optional): Map of attributes and values.


    An :class:`Annotation` represents the text span associated with a label.
    The text spans a number of characters and this is represented
    with the index of its starting and non-inclusive ending character. Span is
    [:attr:`begin`,:attr:`end`). A :class:`Annotation` object is immutable.

    Attributes:
        category (Any, Optional): Label/tag of the text span.

    Note:

        * The :attr:`category` attribute might be removed because container
          already handles annotation categories. Nevertheless, it may be useful
          for when dealing with individual annotations.
    '''

    def __new__(cls, **kwargs: Optional[Dict[str, Any]]):
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

    def copy(self) -> 'UMLSAnnotation':
        return type(self)(**self._asdict())
