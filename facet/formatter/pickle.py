import pickle
from .base import BaseFormatter


__all__ = ['PickleFormatter']


class PickleFormatter(BaseFormatter):

    def _format(self, data):
        return pickle.dumps(data)
