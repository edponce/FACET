import json
from .base import BaseFormatter


__all__ = ['JSONFormatter']


class JSONFormatter(BaseFormatter):

    def _format(self, data):
        return json.dumps(data, indent=2)
