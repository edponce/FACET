from .base import BaseSimilarity
from .dice import DiceSimilarity
from .exact import ExactSimilarity
from .cosine import CosineSimilarity
from .jaccard import JaccardSimilarity
from .overlap import OverlapSimilarity
from .hamming import HammingSimilarity


similarity_map = {
    'dice': DiceSimilarity,
    'exact': ExactSimilarity,
    'cosine': CosineSimilarity,
    'jaccard': JaccardSimilarity,
    'overlap': OverlapSimilarity,
    'hamming': HammingSimilarity,
}
