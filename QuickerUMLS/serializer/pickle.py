import pickle
from .base import BaseSerializer


class PickleSerializer(BaseSerializer):

    def __init__(self, **kwargs):
        self._protocol = kwargs.get('protocol', pickle.HIGHEST_PROTOCOL)
        super().__init__(**kwargs)

    def _serialize(self, obj):
        return pickle.dumps(obj, protocol=self._protocol)

    def _deserialize(self, obj):
        return pickle.loads(obj)
