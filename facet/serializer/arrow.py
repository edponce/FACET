import pyarrow
from .base import BaseSerializer


__all__ = ['ArrowSerializer']


class ArrowSerializer(BaseSerializer):
    """Arrow serializer."""

    NAME = 'arrow'

    def dumps(self, obj):
        return pyarrow.serialize(obj).to_buffer().to_pybytes()

    def loads(self, obj):
        return pyarrow.deserialize(obj)
