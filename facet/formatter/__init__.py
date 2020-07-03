from .base import BaseFormatter
from .json import JSONFormatter
from .yaml import YAMLFormatter
from .xml import XMLFormatter
from .pickle import PickleFormatter
from .csv import CSVFormatter
from .basic import BasicFormatter


formatter_map = {
    'json': JSONFormatter,
    'yaml': YAMLFormatter,
    'xml': XMLFormatter,
    'pickle': PickleFormatter,
    'csv': CSVFormatter,
    'basic': BasicFormatter,
    None: BasicFormatter,
}
