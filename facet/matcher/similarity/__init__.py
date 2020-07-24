from .base import BaseSimilarity
from .dice import DiceSimilarity
from .exact import ExactSimilarity
from .cosine import CosineSimilarity
from .jaccard import JaccardSimilarity
from .overlap import OverlapSimilarity
from .hamming import HammingSimilarity
from typing import Union


similarity_map = {
    DiceSimilarity.NAME: DiceSimilarity,
    ExactSimilarity.NAME: ExactSimilarity,
    CosineSimilarity.NAME: CosineSimilarity,
    JaccardSimilarity.NAME: JaccardSimilarity,
    OverlapSimilarity.NAME: OverlapSimilarity,
    HammingSimilarity.NAME: HammingSimilarity,
}


def get_similarity(value: Union[str, 'BaseSimilarity']):
    if isinstance(value, str):
        return similarity_map[value]()
    elif isinstance(value, BaseSimilarity):
        return value
    raise ValueError(f'invalid similarity measure, {value}')


def get_alpha(value: float):
    """Bound alpha to range [0.01,1]."""
    return min(1, max(value, 0.01))
