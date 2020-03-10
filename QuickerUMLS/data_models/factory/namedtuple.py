from typing import Tuple, Optional, NamedTuple
from collections import namedtuple


def namedtupleFactory(name: str, fields: Tuple, defaults: Optional[Tuple] = None) -> NamedTuple:
    if defaults is None:
        defaults =  len(fields) * (None,)
    return namedtuple(name, fields, defaults=defaults)
