import pickle
import cloudpickle
from .base import BaseFormatter


__all__ = [
    'PickleFormatter',
    'CloudpickleFormatter',
]


class PickleFormatter(BaseFormatter):

    NAME = 'pickle'

    def _format(self, data):
        return pickle.dumps(data)


class CloudpickleFormatter(BaseFormatter):

    NAME = 'cloudpickle'

    def _format(self, data):
        return cloudpickle.dumps(data)
