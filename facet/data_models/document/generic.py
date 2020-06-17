import class_utils
from typing import Any, Union, Optional
from document.encoding import mime_encoding
from factory.namedtuple import namedtupleFactory


_DocumentBase = namedtupleFactory(
    'DocumentBase',
    ('id', 'source', 'category', 'text', 'encoding', 'size'))


# @namedtuple('DocumentBase', ('id', 'source', 'category', 'text', 'encoding', 'size'))
# class Document:
class Document(_DocumentBase):
    '''Representation of text data and associated generic properties.

    Args:
        text (str): Text data.

        kwargs (dict, optional): Map of attributes and values.

    A :class:`Document` object stores text data and associated properties.
    A :class:`Document` always contains valid :attr:`text`.
    A :class:`Document` object is immutable.

    Attributes:
        text (str): Text data.

        id (any, optional): Unique :class:`Document` identifier.

        source (str, optional): Represents the origin of the :attr:`text` data.
            Examples: filename, database-table, and dataset name.

        category(any, optional): Type of :attr:`text` data.
            This attribute is used rarely but might come in handy for
            specific applications.

        encoding (str, optional): Character encoding of :attr:`text` data.
            If libmagic is available (`python-magic` package), it can be
            used to detect the encoding.
            Examples: 'us-ascii' and 'utf-8'.

        size (int): Text span (number of characters in document).
            This attribute is set internally based on :attr:`text`.
    '''

    def __new__(cls, text: str, **kwargs: Optional[Any]):
        # Size and encoding are obtained from valid text.
        # Size argument is ignored.
        if text is None:
            size = 0
            codec = None
        else:
            size = len(text)
            codec = kwargs.get('encoding', mime_encoding(text))
        kwargs['text'] = text
        kwargs['size'] = size
        kwargs['encoding'] = codec
        return super().__new__(cls, **kwargs)

    def __repr__(self) -> str:
        return class_utils.__repr__(self)

    def __str__(self) -> str:
        return class_utils.__str__(self)

    def __len__(self) -> int:
        return self.size

    def __add__(self, doc: Union['Document', str]) -> 'Document':
        '''
        Text of second :class:`Document` is appended to text of first :class:`Document`.
        For all other attributes, the new :class:`Document` will contain the
        same values as the first :class:`Document`.
        '''
        return self.append(doc)

    def encode(self, codec: str = 'utf-8') -> 'Document':
        attrs = self._asdict()
        if self.encoding != codec:
            attrs['text'] = self.text.encode(codec)
            attrs['encoding'] = codec
        return type(self)(**attrs)
        # attrs = {}
        # if self.encoding != codec:
        #     attrs['text'] = self.text.encode(codec)
        #     attrs['encoding'] = codec
        # return self._replace(**attrs)

    def decode(self) -> 'Document':
        attrs = self._asdict()
        attrs['text'] = self.text.decode(self.encoding)
        attrs['encoding'] = mime_encoding(attrs['text'])
        return type(self)(**attrs)

    def append(self, text: Union['Document', str], **kwargs) -> 'Document':
        '''
        Text of second :class:`Document` (or string) is appended to text of
        first :class:`Document`.
        If id and source attributes are not provided, then values from the
        first :class:`Document` are used.
        Assume both Documents have same encodings (no validation is performed).
        '''
        attrs = self._asdict()
        if isinstance(text, type(self)):
            text = text.text
        attrs['text'] = attrs.get('text', '') + text
        attrs['id'] = kwargs.get('id', self.id)
        attrs['source'] = kwargs.get('source', self.source)
        return type(self)(**attrs)

    def copy(self) -> 'Document':
        return type(self)(**self._asdict())
