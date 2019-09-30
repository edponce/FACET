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

    def dumps(self, obj: str):
        return self.encode(obj)

    def loads(self, obj) -> str:
        return self.decode(obj)


class StringSJSerializer(BaseSerializer):
    """String split-join serializer for sequence-like objects.

    Args:
        delimiter (str): Character to delimit strings.
    """

    def __init__(self, **kwargs):
        self._delimiter = kwargs.get('delimiter', '|')
        super().__init__(**kwargs)

    def dumps(self, obj: Iterable[str]):
        return self.encode(self._delimiter.join(obj))

    def loads(self, obj) -> List[str]:
        return self.decode(obj).split(self._delimiter)
