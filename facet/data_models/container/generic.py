import os
import container.utils
from typing import Any, Optional
from factory.namedtuple import namedtupleFactory


_ContainerBase = namedtupleFactory('ContainerBase',
    ('category', 'elements'))


class Container(_ContainerBase):
    '''Container for arbitrary objects.

    Args:
        category(any, optional): Category of container.

        kwargs (dict, optional): Map of attributes.

    A :class:`Container` object is immutable.
    Attribute :attr:`elements` is a set because it is mutable,
    no duplicate, no index, no order.

    Attributes:
        category(any, optional): Category of container.

        elements(set, optional): Set of Documents.
    '''

    def __new__(cls, dkey='elements', **kwargs: Optional[Any]):
        elements = set()
        for e in kwargs.get(dkey, elements):
            elements.add(e)
        kwargs['elements'] = elements
        return super().__new__(cls, **kwargs)

    def __repr__(self):
        return container.utils.__repr__(self)

    def __str__(self):
        return container.utils.__str__(self,
                                       key='elements',
                                       value=self.elements)

    def __len__(self):
        return len(self.elements)

    def add(self, element: Any, dtype: Any = None):
        if dtype is not None:
            if not isinstance(element, dtype):
                raise TypeError(f'not a valid {dtype.__qualname__} object')
        self.elements.add(element)

    def __iter__(self):
        yield from self.elements
