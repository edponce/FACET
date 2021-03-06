import yaml
from .base import BaseFormatter


__all__ = ['YAMLFormatter']


class YAMLFormatter(BaseFormatter):

    NAME = 'yaml'

    def _format(self, data):
        return yaml.dump(data)
