import copy
from .base import BaseFacet
from .facet import Facet
from .umls import UMLSFacet
from .formatter import formatter_map
from .tokenizer import tokenizer_map
from .database import database_map
from .serializer import serializer_map
from .simstring import simstring_map
from .simstring.similarity import similarity_map
from .simstring.ngram import ngram_map
from .utils import load_configuration
from typing import (
    Any,
    Dict,
    Union,
)


__all__ = ['FacetFactory']


facet_map = {
    None: Facet,  # default
    'facet': Facet,
    'umlsfacet': UMLSFacet,
}

OBJTYPE_CLASSMAP_MAP = {
    'tokenizer': tokenizer_map,
    'formatter': formatter_map,
    'database': database_map,
    'simstring': simstring_map,
    'similarity': similarity_map,
    'ngram': ngram_map,
    'serializer': serializer_map,
}

# NOTE: For all classes, map parameter names that support objects to
# their associated object type. If the OBJ_CLASS_LABEL exists as an attribute
# then it is considered as an object. Only parameters that have a different
# name from the object type are listed here.
PARAM_OBJTYPE_MAP = {
    'db': 'database',         # Simstring database
    'cache_db': 'database',   # Simstring cache database
    'cuisty_db': 'database',  # (UMLSFacet) CUI-STY database
    'conso_db': 'database',   # (UMLSFacet) CONCEPT-CUI database
}


class FacetFactory:
    """Generate a FACET instance from a given configuration."""

    def __init__(
        self,
        config: Union[str, Dict[str, Any]],
        *,
        section: str = None,
        # Configuration keyword used to specify classes
        class_label: str = 'class',
    ):
        if isinstance(config, str) and ':' in config:
            config, section = config.split(':')
        self._config = load_configuration(config, keys=section)
        self._OBJ_CLASS_LABEL = class_label

    def create(self) -> 'BaseFacet':
        """Create a new FACET instance."""
        if self._config:
            # NOTE: Copy configuration because '_parse_config()' modifies it.
            config = copy.deepcopy(self._config)

        obj_class = (
            config.pop(self._OBJ_CLASS_LABEL).lower()
            if self._OBJ_CLASS_LABEL in config
            else None
        )
        return facet_map[obj_class](**self._parse_config(config))

    __call__ = create

    def create_generic(self) -> Any:
        """Create a new instance of any FACET-related classes."""
        # NOTE: Copy configuration because '_parse_config()' modifies it.
        if self._config:
            config = copy.deepcopy(self._config)
        objs = [v for k, v in self._parse_config(config).items()]
        return objs if len(objs) > 1 else objs[0]

    def _parse_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Recusively parse a configuration map."""
        factory_config = {}
        for k, v in config.items():
            # It is an object, with class and parameters
            if isinstance(v, dict):
                # If parameter is a dictionary not representing an object.
                if self._OBJ_CLASS_LABEL in v:
                    # NOTE: If a parameter name matches a key in
                    # PARAM_OBJTYPE_MAP but it is not actually an
                    # object-based parameter.
                    obj_type = PARAM_OBJTYPE_MAP.get(k, k)
                    obj_class = v.pop(self._OBJ_CLASS_LABEL).lower()
                    if (
                        obj_type in OBJTYPE_CLASSMAP_MAP
                        and obj_class in OBJTYPE_CLASSMAP_MAP[obj_type]
                    ):
                        obj_params = self._parse_config(v)
                        v = OBJTYPE_CLASSMAP_MAP[obj_type][obj_class](
                            **obj_params
                        )
                else:
                    v = self._parse_config(v)

            # It is an object, by default
            elif isinstance(v, str):
                # NOTE: If a parameter name matches a key in
                # PARAM_OBJTYPE_MAP but it is not actually an
                # object-based parameter.
                obj_type = PARAM_OBJTYPE_MAP.get(k, k)
                obj_class = v.lower()
                if (
                    obj_type in OBJTYPE_CLASSMAP_MAP
                    and obj_class in OBJTYPE_CLASSMAP_MAP[obj_type]
                ):
                    v = OBJTYPE_CLASSMAP_MAP[obj_type][obj_class]()

            factory_config[k] = v
        return factory_config
