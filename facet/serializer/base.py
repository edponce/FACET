import unicodedata
from typing import Any
from abc import (
    ABC,
    abstractmethod,
)


__all__ = ['BaseSerializer']


class BaseSerializer(ABC):
    """Interface for Serializer instances.

    Notes:
        * Follows a mixin classes approach - parent class supplies helper
          methods that depend on augment capabilities supplied in subclass.
    """

    def __init__(self, **kwargs):
        """
        Args:
            form (str): Valid values are 'NFC', 'NFKC', 'NFD', and 'NFKD'.
        """
        # NOTE: Set encoding and normalization parameters as data members
        # to guarantee that an instance is able to operate on any object
        # it operated on previously.
        self._encoding = kwargs.get('encoding', 'utf-8')
        self._form = kwargs.get('form', 'NFKD')

    def encode(self, string: str) -> bytes:
        """Function wrapper over str.encode()."""
        return string.encode(encoding=self._encoding)

    def decode(self, string_b: bytes) -> str:
        """Function wrapper over str.decode()."""
        return string_b.decode(encoding=self._encoding)

    def normalize_unicode(self, string_u: str) -> str:
        """Convert Unicode string to a normal form."""
        return unicodedata.normalize(self._form, string_u)

    @abstractmethod
    def dumps(self, obj: Any) -> bytes:
        """Serialize arbitrary parameter."""
        pass

    @abstractmethod
    def loads(self, obj: bytes) -> Any:
        """Deserialize parameter to arbitrary object."""
        pass
