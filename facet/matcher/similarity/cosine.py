import math
from .base import BaseSimilarity


__all__ = ['CosineSimilarity']


class CosineSimilarity(BaseSimilarity):
    """Cosine similarity.

    Conditions:
        >>> cosine(x,y) = |x & y| / sqrt(|x||y|), (0,1] -> R
        >>> cosine(x,y) >= a
        >>> ceil(a*sqrt(|x||y|)) <= |x & y| <= min(|x|,|y|)
        >>> ceil(a^2*|x||y|) <= |y|^2
        >>> ceil(a^2*|x|) <= |y| <= floor(|x|/a^2)
    """

    NAME = 'cosine'

    def min_features(self, length, alpha):
        return int(math.ceil(alpha * alpha * length))

    def max_features(self, length, alpha):
        return int(math.floor(length / (alpha * alpha)))

    def min_common_features(self, lengthA, lengthB, alpha):
        return int(math.ceil(alpha * math.sqrt(lengthA * lengthB)))

    def similarity(self, featuresA, featuresB):
        fa = set(featuresA)
        fb = set(featuresB)
        return len(fa & fb) / math.sqrt(len(fa) * len(fb))
