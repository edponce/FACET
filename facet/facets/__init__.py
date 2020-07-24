from .base import BaseFacet
from .facet import Facet
from .umls import UMLSFacet
from typing import Union


facet_map = {
    'facet': Facet,
    'umlsfacet': UMLSFacet,
    None: Facet,
}


def get_facet(value: Union[str, 'BaseFacet']):
    if value is None or isinstance(value, str):
        return facet_map[value]()
    elif isinstance(value, BaseFacet):
        return value
    raise ValueError(f'invalid FACET, {value}')
