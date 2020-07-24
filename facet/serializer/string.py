from .base import BaseSerializer
from typing import (
    List,
    Iterable,
)


__all__ = [
    'StringSerializer',
    'StringSJSerializer',
]


class StringSerializer(BaseSerializer):
    """Simple string serializer."""

    NAME = 'string'

    def dumps(self, obj: str):
        return self.encode(obj)

    def loads(self, obj) -> str:
        return self.decode(obj)


class StringSJSerializer(BaseSerializer):
    """String split-join serializer for sequence-like objects.

    Args:
        delimiter (str): Character to delimit strings.
    """

    NAME = 'stringsj'

    def __init__(self, *, delimiter='|', **kwargs):
        super().__init__(**kwargs)
        self._delimiter = delimiter

    def dumps(self, obj: Iterable[str]):
        return self.encode(self._delimiter.join(obj))

    def loads(self, obj) -> List[str]:
        return self.decode(obj).split(self._delimiter)
