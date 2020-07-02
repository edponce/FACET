from .base import BaseFacet
from .facet import Facet
from .umls_facet import UMLSFacet


facet_map = {
    None: Facet,
    'facet': Facet,
    'umlsfacet': UMLSFacet,
}
