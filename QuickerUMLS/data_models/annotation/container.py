from typing import Any, Optional
from container.generic import Container
from annotation.generic import Annotation


class Annotations(Container):
    '''Container for annotations.

    Args:
        kwargs (dict, optional): Map of attributes.

    An :class:`Annotations` object is immutable.
    Attribute :attr:`annotations` is a set because it is mutable,
    no duplicate, no index, no order.

    Attributes:
        category(any, optional): Category of :attr:`annotations` data.

        annotations(set, optional): Annotation set.
    '''

    annotations = Container.elements

    def __new__(cls, **kwargs: Optional[Any]):
        return super().__new__(cls, dkey='annotations', **kwargs)

    def add(self, annotation: 'Annotation'):
        super().add(annotation, dtype=Annotation)
