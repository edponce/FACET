from .base import BaseFacet
from .facet import Facet
from .umls import UMLSFacet


facet_map = {
    None: Facet,
    'facet': Facet,
    'umlsfacet': UMLSFacet,
}
