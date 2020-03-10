from typing import Any, Optional
from document.generic import Document
from container.generic import Container


class Documents(Container):
    '''Container for documents.

    Args:
        kwargs (dict, optional): Map of attributes.

    A :class:`Documents` object is immutable.
    Attribute :attr:`documents` is a set because it is mutable,
    no duplicate, no index, no order.

    Attributes:
        category(any, optional): Category of :attr:`documents` data.

        documents(set, optional): Document set.
    '''

    documents = Container.elements

    def __new__(cls, **kwargs: Optional[Any]):
        return super().__new__(cls, dkey='documents', **kwargs)

    def add(self, document: 'Document'):
        super().add(document, dtype=Document)
