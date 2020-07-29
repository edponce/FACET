import Levenshtein
from typing import (
    Union,
    Callable,
)


# A 'distance' refers to a
# callable/function that takes a pair of
# strings as its first parameters and
# calculates a distance metric in [0,1].
distance_map = {
    # Distance ratio = (sum length of strings - Levenshtein distance) /
    #                  (sum length of strings)
    # Levenshtein distance penalties: insert = 1, delete = 1, replace = 2
    'levenshtein': Levenshtein.ratio,
    'jaro': Levenshtein.jaro,
    'jaro-winkler': Levenshtein.jaro_winkler,
    # NOTE: Set to highest value so that
    # it is not ignored due to threshold
    # filtering.
    None: lambda x, y: 1.0,
}


def get_distance(value: Union[str, Callable]) -> Callable:
    if isinstance(value, str):
        return distance_map[value]
    elif callable(value):
        return value
    raise ValueError(f'invalid distance measure, {value}')
