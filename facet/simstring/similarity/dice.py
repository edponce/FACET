import math
from .base import BaseSimilarity


__all__ = ['DiceSimilarity']


class DiceSimilarity(BaseSimilarity):
    """Sorensen-Dice coefficient."""

    # Conditions:
    #
    #     * dice(x,y) = 2|x & y| / (|x| + |y|), (0,1] -> R
    #     * dice(x,y) >= a
    #     * ceil(1/2*a*(|x|+|y|)) <= |x & y| <= min(|x|,|y|)
    #     * ceil(a/(2-a)*|x|) <= |y| <= floor((2-a)/a*|x|)

    _name = 'dice'

    def min_features(self, length, alpha):
        return int(math.ceil(alpha / (2. - alpha) * length))

    def max_features(self, length, alpha):
        return int(math.floor((2. - alpha) / alpha * length))

    def min_common_features(self, lengthA, lengthB, alpha):
        return int(math.ceil(.5 * alpha * (lengthA + lengthB)))

    def similarity(self, featuresA, featuresB):
        fa = set(featuresA)
        fb = set(featuresB)
        return float(2. * len(fa & fb) / (len(fa) + len(fb)))
