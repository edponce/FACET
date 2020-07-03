from .base import BaseFormatter


__all__ = ['BasicFormatter']


class BasicFormatter(BaseFormatter):

    def _format(self, data):
        return data
