from .base import BaseFacet
from .facet import Facet
from .parallel import ParallelFacet
from .umls import UMLSFacet
from typing import Union


facet_map = {
    Facet.NAME: Facet,
    ParallelFacet.NAME: ParallelFacet,
    UMLSFacet.NAME: UMLSFacet,
    None: Facet,
}
