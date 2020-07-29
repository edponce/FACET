import math
from .base import BaseSimilarity


__all__ = ['OverlapSimilarity']


class OverlapSimilarity(BaseSimilarity):
    """Overlap coefficient (Szymkiewicz-Simpson coefficient).

    Conditions:
        >>> overlap(x,y) = |x & y| / min(|x|,|y|), (0,1] -> R
        >>> overlap(x,y) >= a
        >>> ceil(a*min(|x|,|y|)) <= |x & y| <= min(|x|,|y|)
        >>> ceil(a*|y|) <= |y|
        >>> ceil(a) <= 1 <= floor(1/a)
        >>> ceil(a*|y|) <= |y| <= floor(|y|/a)
        >>> ceil(a*|x|) <= |x| <= floor(|x|/a)
    """

    NAME = 'overlap'

    def min_features(self, length, alpha):
        return int(math.ceil(alpha * length))

    def max_features(self, length, alpha):
        return int(math.floor(length / alpha))

    def min_common_features(self, lengthA, lengthB, alpha):
        return int(math.ceil(alpha * min(lengthA, lengthB)))

    def similarity(self, featuresA, featuresB):
        fa = set(featuresA)
        fb = set(featuresB)
        return len(fa & fb) / min(len(fa), len(fb))
