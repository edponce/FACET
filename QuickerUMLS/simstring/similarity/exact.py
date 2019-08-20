import math
from .base import BaseSimilarity


__all__ = ['ExactSimilarity']


class ExactSimilarity(BaseSimilarity):

    def min_features(self, length, alpha):
        return int(math.ceil(alpha * alpha * length))

    def max_features(self, length, alpha):
        return int(math.floor(length / (alpha * alpha)))

    def min_common_features(self, lengthA, lengthB, alpha):
        return int(math.ceil(alpha * math.sqrt(lengthA * lengthB)))

    def similarity(self, featuresA, featuresB):
        fa = set(featuresA)
        fb = set(featuresB)
        return float(fa == fb)
