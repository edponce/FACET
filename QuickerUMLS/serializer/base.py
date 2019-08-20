import unicodedata
from typing import Any
from abc import ABC, abstractmethod


__all__ = ['BaseSerializer']


class BaseSerializer(ABC):

    def __init__(self, **kwargs):
        """
        Args:
            form (str): Valid values are 'NFC', 'NFKC', 'NFD', and 'NFKD'.
        """
        # NOTE: Set parameters at initialization to ensure that an instance
        # is able to operate on any object it operated on previously.
        self._encoding = kwargs.get('encoding', 'utf-8')
        self._form = kwargs.get('form', 'NFKD')

    def _encode(self, string: str) -> bytes:
        """Function wrapper over str.encode()."""
        return string.encode(encoding=self._encoding)

    def _decode(self, string_b: bytes) -> str:
        """Function wrapper over str.decode()."""
        return string_b.decode(encoding=self._encoding)

    def normalize_unicode(self, string_u: str) -> str:
        """Conver Unicode string to a normal form."""
        return unicodedata.normalize(self._form, string_u)

    @abstractmethod
    def _serialize(cls, obj: Any) -> bytes:
        pass

    @abstractmethod
    def _deserialize(cls, obj: bytes) -> Any:
        pass

    def dumps(self, obj: Any) -> bytes:
        """Serialize arbitrary argument."""
        if isinstance(obj, str):
            obj = self._encode(obj)
        elif not isinstance(obj, bytes):
            obj = self._serialize(obj)
        return obj

    def loads(self, obj: bytes) -> Any:
        """Deserialize argument to arbitrary object."""
        try:
            obj = self._decode(obj)
        except Exception:
            obj = self._deserialize(obj)
        return obj
