import pickle
from .base import BaseFormatter


__all__ = ['PickleFormatter']


class PickleFormatter(BaseFormatter):

    NAME = 'pickle'

    def _format(self, data):
        return pickle.dumps(data)
