from . import (
    CosineSimilarity,
    DiceSimilarity,
    JaccardSimilarity,
    OverlapSimilarity,
    ExactSimilarity,
    HammingSimilarity,
)


def get_similarity_measure(measure: str):
    """Get similarity measure instance based on name.

    Args:
        measure (str): Similarity measure name. Valid measures are: 'cosine',
            'jaccard', 'dice', 'exact', 'overlap', 'hamming'.
    """
    if measure == 'cosine':
        return CosineSimilarity()
    elif measure == 'dice':
        return DiceSimilarity()
    elif measure == 'jaccard':
        return JaccardSimilarity()
    elif measure == 'overlap':
        return OverlapSimilarity()
    elif measure == 'exact':
        return ExactSimilarity()
    elif measure == 'hamming':
        return HammingSimilarity()
