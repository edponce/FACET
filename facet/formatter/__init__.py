from .base import BaseFormatter
from .json import JSONFormatter
from .yaml import YAMLFormatter
from .xml import XMLFormatter
from .pickle import PickleFormatter
from .csv import CSVFormatter
from .null import NullFormatter
from typing import Union


formatter_map = {
    JSONFormatter.NAME: JSONFormatter,
    YAMLFormatter.NAME: YAMLFormatter,
    XMLFormatter.NAME: XMLFormatter,
    PickleFormatter.NAME: PickleFormatter,
    CSVFormatter.NAME: CSVFormatter,
    NullFormatter.NAME: NullFormatter,
    None: NullFormatter,
}


def get_formatter(value: Union[str, 'BaseFormatter']):
    if value is None or isinstance(value, str):
        return formatter_map[value]()
    elif isinstance(value, BaseFormatter):
        return value
    raise ValueError(f'invalid formatter, {value}')
