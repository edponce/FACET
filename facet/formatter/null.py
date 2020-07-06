from .base import BaseFormatter


__all__ = ['NullFormatter']


class NullFormatter(BaseFormatter):

    def _format(self, data):
        return data
