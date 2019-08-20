import pickle
from .base import BaseSerializer


__all__ = ['PickleSerializer']


class PickleSerializer(BaseSerializer):
    """Pickle serializer.

    Args:
        protocol (int): Protocol version to use.
            Default is pickle.HIGHEST_PROTOCOL.
    """

    def __init__(self, **kwargs):
        self._protocol = kwargs.get('protocol', pickle.HIGHEST_PROTOCOL)
        super().__init__(**kwargs)

    def _serialize(self, obj):
        return pickle.dumps(obj, protocol=self._protocol)

    def _deserialize(self, obj):
        return pickle.loads(obj)
