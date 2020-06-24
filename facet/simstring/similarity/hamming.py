import math
from .base import BaseSimilarity


__all__ = ['HammingSimilarity']


class HammingSimilarity(BaseSimilarity):
    """Hamming distance."""

    # Conditions:
    #
    #     * hamming(x,y) = |(x-y) U (y-x)|, (0,1] -> R
    #     * hamming(x,y) >= a
    #     * ceil(a*(|x|+|y|)) <= 1 <= min(|x|,|y|)
    #     * ceil(a*|y|) <= 1 <= floor(|y|/a)

    def min_features(self, length, alpha):
        return int(math.ceil(alpha * length))

    def max_features(self, length, alpha):
        return int(math.floor(length / alpha))

    def min_common_features(self, lengthA, lengthB, alpha):
        return int(math.ceil(alpha * (lengthA + lengthB)))

    def similarity(self, featuresA, featuresB):
        fa = set(featuresA)
        fb = set(featuresB)
        return float(len(fa ^ fb))
