import copy
from .facets import facet_map
from .formatter import formatter_map
from .tokenizer import tokenizer_map
from .database import database_map
from .serializer import serializer_map
from .matcher import matcher_map
from .matcher.similarity import similarity_map
from .matcher.ngram import ngram_map
from .utils import load_configuration
from typing import (
    Any,
    Dict,
    Union,
)


__all__ = ['FacetFactory']


class FacetFactory:
    """Generate a FACET instance from a given configuration."""

    OBJTYPE_CLASSMAP_MAP = {
        'tokenizer': tokenizer_map,
        'formatter': formatter_map,
        'database': database_map,
        'matcher': matcher_map,
        'similarity': similarity_map,
        'ngram': ngram_map,
        'serializer': serializer_map,
    }

    # NOTE: For all classes, map parameter names that support objects to their
    # associated object type. If the OBJ_CLASS_LABEL exists as an attribute
    # then it is considered as an object. Only parameters that have a different
    # name from the object type are listed here.
    PARAM_OBJTYPE_MAP = {
        'db': 'database',         # Matcher database
        'cache_db': 'database',   # Matcher cache database
        'cuisty_db': 'database',  # (UMLSFacet) CUI-STY database
        'conso_db': 'database',   # (UMLSFacet) CONCEPT-CUI database
    }

    # Configuration keyword used to specify classes.
    # Keyword can be modified in case it is the same as a class parameter.
    OBJ_CLASS_LABEL = 'class'

    def __init__(
        self,
        config: Union[str, Dict[str, Any]],
        *,
        section: str = None,
    ):
        self._config = load_configuration(config, keys=section)

    def create(self):
        """Create a new FACET instance."""
        if self._config:
            # NOTE: Copy configuration because '_parse_config()' modifies it.
            config = copy.deepcopy(self._config)

        obj_class = (
            config.pop(type(self).OBJ_CLASS_LABEL).lower()
            if type(self).OBJ_CLASS_LABEL in config
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
                # If parameter is a dictionary representing an object.
                if type(self).OBJ_CLASS_LABEL in v:
                    # NOTE: If a parameter name matches a key in
                    # PARAM_OBJTYPE_MAP but it is not actually an
                    # object-based parameter.
                    obj_type = type(self).PARAM_OBJTYPE_MAP.get(k, k)
                    obj_class = v.pop(type(self).OBJ_CLASS_LABEL).lower()
                    if (
                        obj_type in type(self).OBJTYPE_CLASSMAP_MAP
                        and obj_class in type(self).OBJTYPE_CLASSMAP_MAP[obj_type]
                    ):
                        obj_params = self._parse_config(v)
                        v = type(self).OBJTYPE_CLASSMAP_MAP[obj_type][obj_class](**obj_params)
                else:
                    v = self._parse_config(v)

            # It is an object, by default
            elif isinstance(v, str):
                # NOTE: If a parameter name matches a key in
                # PARAM_OBJTYPE_MAP but it is not actually an
                # object-based parameter.
                obj_type = type(self).PARAM_OBJTYPE_MAP.get(k, k)
                obj_class = v.lower()
                if (
                    obj_type in type(self).OBJTYPE_CLASSMAP_MAP
                    and obj_class in type(self).OBJTYPE_CLASSMAP_MAP[obj_type]
                ):
                    v = type(self).OBJTYPE_CLASSMAP_MAP[obj_type][obj_class]()

            factory_config[k] = v
        return factory_config
