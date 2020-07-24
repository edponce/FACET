import math
from .base import BaseSimilarity


__all__ = ['JaccardSimilarity']


class JaccardSimilarity(BaseSimilarity):
    """Jaccard similarity coefficient."""

    # Conditions:
    #
    #     * jaccard(x,y) = |x & y| / |x U y|, (0,1] -> R
    #     * jaccard(x,y) >= a
    #     * ceil(a*|x U y|) <= |x & y| <= min(|x|,|y|)
    #     * ceil(a*|x U y|) <= |y|
    #     * ceil(a*|x|) <= |y| <= floor(|x|/a)

    NAME = 'jaccard'

    def min_features(self, length, alpha):
        return int(math.ceil(alpha * length))

    def max_features(self, length, alpha):
        return int(math.floor(length / alpha))

    def min_common_features(self, lengthA, lengthB, alpha):
        return int(math.ceil(alpha * (lengthA + lengthB) / (1. + alpha)))

    def similarity(self, featuresA, featuresB):
        fa = set(featuresA)
        fb = set(featuresB)
        return float(len(fa & fb) / len(fa | fb))
