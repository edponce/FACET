from .base import BaseFormatter


__all__ = ['SimpleFormatter']


class SimpleFormatter(BaseFormatter):

    def _format(self, data):
        return data
