import math
from .base import BaseSimilarity


__all__ = ['JaccardSimilarity']


class JaccardSimilarity(BaseSimilarity):

    def min_features(self, length, alpha):
        return int(math.ceil(alpha * length))

    def max_features(self, length, alpha):
        return int(math.floor(length / alpha))

    def min_common_features(self, lengthA, lengthB, alpha):
        return int(
            math.ceil(alpha * (lengthA + lengthB) / (1.0 + alpha))
        )

    def similarity(self, featuresA, featuresB):
        fa = set(featuresA)
        fb = set(featuresB)
        return float(len(fa & fb) / len(fa | fb))
