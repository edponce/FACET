from ruamel import yaml
from .base import BaseSerializer


__all__ = ['YAMLSerializer']


class YAMLSerializer(BaseSerializer):
    """YAML serializer."""

    def dumps(self, obj):
        return yaml.dump(obj)

    def loads(self, obj):
        return yaml.safe_load(obj)
