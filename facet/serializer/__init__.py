from .base import BaseSerializer
from .json import JSONSerializer
from .yaml import YAMLSerializer
from .pickle import PickleSerializer
from .string import (
    StringSerializer,
    StringSJSerializer,
)


serializer_map = {
    'json': JSONSerializer,
    'yaml': YAMLSerializer,
    'pickle': PickleSerializer,
    'string': StringSerializer,
    'stringsj': StringSJSerializer,
}
