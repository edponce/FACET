from .base import BaseSerializer
from .json import JSONSerializer
from .yaml import YAMLSerializer
from .arrow import ArrowSerializer
from .pickle import (
    PickleSerializer,
    CloudpickleSerializer,
)
from .string import (
    StringSerializer,
    StringSJSerializer,
)
from .null import NullSerializer
from typing import Union


serializer_map = {
    JSONSerializer.NAME: JSONSerializer,
    YAMLSerializer.NAME: YAMLSerializer,
    ArrowSerializer.NAME: ArrowSerializer,
    PickleSerializer.NAME: PickleSerializer,
    CloudpickleSerializer.NAME: CloudpickleSerializer,
    StringSerializer.NAME: StringSerializer,
    StringSJSerializer.NAME: StringSJSerializer,
    NullSerializer.NAME: NullSerializer,
    None: NullSerializer,
}


def get_serializer(value: Union[str, 'BaseSerializer']):
    if value is None or isinstance(value, str):
        return serializer_map[value]()
    elif isinstance(value, BaseSerializer):
        return value
    raise ValueError(f'invalid serializer, {value}')
