from .base import BaseSerializer


__all__ = ['NullSerializer']


class NullSerializer(BaseSerializer):
    """Dummy serializer with no effect."""

    def dumps(self, obj):
        return obj

    def loads(self, obj):
        return obj
