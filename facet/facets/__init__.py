from .base import BaseFacet
from .facet import Facet
from .umls import UMLSFacet
from typing import Union


facet_map = {
    'facet': Facet,
    'umlsfacet': UMLSFacet,
    None: Facet,
}
