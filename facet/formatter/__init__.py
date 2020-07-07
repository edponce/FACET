from .base import BaseFormatter
from .json import JSONFormatter
from .yaml import YAMLFormatter
from .xml import XMLFormatter
from .pickle import PickleFormatter
from .csv import CSVFormatter
from .null import NullFormatter


formatter_map = {
    'json': JSONFormatter,
    'yaml': YAMLFormatter,
    'xml': XMLFormatter,
    'pickle': PickleFormatter,
    'csv': CSVFormatter,
    'null': NullFormatter,
    None: NullFormatter,
}
