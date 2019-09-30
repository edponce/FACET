import json
from .base import BaseSerializer


__all__ = ['JSONSerializer']


class JSONSerializer(BaseSerializer):
    """JSON serializer."""

    def dumps(self, obj):
        return json.dump(obj)

    def loads(self, obj):
        return json.load(obj)
