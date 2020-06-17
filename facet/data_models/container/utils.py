import os
from typing import Iterable, Optional


def __repr__(obj, sep: Optional[str] = ', ') -> str:
    '''Default representation of a container object.

    Assumes 'obj' is an instance of namedtuple.

    Args:
        sep (str, optional): String separate object fields.
    '''
    ss = []
    for f in obj._fields:
        v = getattr(obj, f)
        if isinstance(v, str):
            ss.append(f"{f}='{v}'")
        else:
            ss.append(f'{f}={v}')
    return f'{type(obj).__qualname__}' + '(' + sep.join(ss) + ')'


def __str__(obj, key: str, value: Iterable, sep: Optional[str] = os.linesep) -> str:
    '''Pretty print a container object.

    Assumes 'obj' is an instance of namedtuple.

    Args:
        key (str): Attribute name of container elements.

        value (Iterable): Collection of container elements (need to support str()).

        sep (str, optional): String to separate object fields.
    '''
    ss = []
    for f in obj._fields:
        # Skip the elements field
        if f in key:
            continue
        v = getattr(obj, f)
        if isinstance(v, str):
            ss.append(f"{f}='{v}'")
        else:
            ss.append(f'{f}={v}')
    for v in value:
        ss.append(str(v))
    return sep.join(ss)
