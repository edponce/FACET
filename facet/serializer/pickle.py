import pickle
from .base import BaseSerializer


__all__ = ['PickleSerializer']


class PickleSerializer(BaseSerializer):
    """Pickle serializer.

    Args:
        protocol (int): Protocol version to use.
    """

    NAME = 'pickle'

    def __init__(self, *, protocol=pickle.HIGHEST_PROTOCOL, **kwargs):
        super().__init__(**kwargs)
        self._protocol = protocol

    def dumps(self, obj):
        return pickle.dumps(obj, protocol=self._protocol)

    def loads(self, obj):
        return pickle.loads(obj)
