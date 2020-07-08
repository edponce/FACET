from .base import BaseSimilarity


__all__ = ['ExactSimilarity']


class ExactSimilarity(BaseSimilarity):
    """Exact similarity."""

    # Conditions:
    #
    #     * exact(x,y) = x == y, [True, False] -> R

    def min_features(self, length, alpha):
        return int(length)

    def max_features(self, length, alpha):
        return int(length)

    def min_common_features(self, lengthA, lengthB, alpha):
        return int(max(lengthA, lengthB))

    def similarity(self, featuresA, featuresB):
        fa = set(featuresA)
        fb = set(featuresB)
        return float(fa == fb)
