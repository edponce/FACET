import math
from .base import BaseSimilarity


__all__ = ['DiceSimilarity']


class DiceSimilarity(BaseSimilarity):

    def min_features(self, length, alpha):
        return int(math.ceil(alpha / (2.0 - alpha) * length))

    def max_features(self, length, alpha):
        return int(math.floor((2.0 - alpha) * length / alpha))

    def min_common_features(self, lengthA, lengthB, alpha):
        return int(math.ceil(0.5 * alpha * lengthA * lengthB))

    def similarity(self, featuresA, featuresB):
        fa = set(featuresA)
        fb = set(featuresB)
        return float(len(fa & fb) * 2.0 / (len(fa) + len(fb)))
