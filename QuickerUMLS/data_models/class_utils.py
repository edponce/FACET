import os
from typing import Optional


def __repr__(obj, sep: Optional[str] = ', ') -> str:
    '''Default representation of an object.

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


def __str__(obj, sep: Optional[str] = os.linesep) -> str:
    '''Pretty print an object.

    Assumes 'obj' is an instance of namedtuple.

    Args:
        sep (str, optional): String to separate object fields.
    '''
    ss = []
    for f in obj._fields:
        v = getattr(obj, f)
        if isinstance(v, str):
            ss.append(f"{f}='{v}'")
        else:
            ss.append(f'{f}={v}')
    return sep.join(ss)
