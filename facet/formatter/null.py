from .base import BaseFormatter


__all__ = ['NullFormatter']


class NullFormatter(BaseFormatter):

    NAME = 'null'

    def _format(self, data):
        return data
