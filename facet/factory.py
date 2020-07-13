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
    """Creates a FACET instance from a given configuration."""

    _DEFAULT_OBJ = 'facet'

    _OBJTYPE_CLASSMAP_MAP = {
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
    _PARAM_OBJTYPE_MAP = {
        'db': 'database',         # Matcher database
        'cache_db': 'database',   # Matcher cache database
        'cuisty_db': 'database',  # (UMLSFacet) CUI-STY database
        'conso_db': 'database',   # (UMLSFacet) CONCEPT-CUI database
    }

    # NOTE: These are CLI parameters that should be removed so that factory
    # can be use programatically using the same CLI-based configuration files.
    _INVALID_KEYS = {
        'query',
        'output',
        'install',
        'host',
        'port',
        'dump_config',
    }

    # Configuration keyword used to specify classes.
    # Keyword can be modified in case it is the same as a class parameter.
    _OBJ_CLASS_LABEL = 'class'

    def __init__(
        self,
        config: Union[str, Dict[str, Any]] = None,
        *,
        section: str = None,
    ):
        self._config = load_configuration(config, keys=section)
        # NOTE: Remove CLI-specific parameters
        for key in type(self)._INVALID_KEYS:
            if key in self._config:
                del self._config[key]

    def get_config(self):
        return self._config

    def get_class(self):
        """Returns the FACET class type.

        Notes:
            * Useful to set the 'target_class' parameter for
              facet.network.SocketClient
        """
        if 'class' in self._config:
            return facet_map[self._config['class']]

    def create(self):
        """Create a new FACET instance."""
        # NOTE: Copy configuration because '_parse_config_map()' modifies it.
        config = copy.deepcopy(self._config)
        obj_class = (
            config.pop(type(self)._OBJ_CLASS_LABEL).lower()
            if type(self)._OBJ_CLASS_LABEL in config
            else type(self)._DEFAULT_OBJ
        )
        return facet_map[obj_class](**self._parse_config_map(config))

    __call__ = create

    def create_generic(self) -> Any:
        """Create a new instance of any FACET-related classes."""
        # NOTE: Copy configuration because '_parse_config_map()' modifies it.
        config = copy.deepcopy(self._config)
        objs = [v for k, v in self._parse_config_map(config).items()]
        return objs if len(objs) > 1 else objs[0]

    def _parse_config_map(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Recusively parse a configuration map."""
        factory_config = {}
        for k, v in config.items():
            # It is an object, with class and parameters
            if isinstance(v, dict):
                # If parameter is a dictionary representing an object.
                if type(self)._OBJ_CLASS_LABEL in v:
                    # NOTE: If a parameter name matches a key in
                    # PARAM_OBJTYPE_MAP but it is not actually an
                    # object-based parameter.
                    obj_type = type(self)._PARAM_OBJTYPE_MAP.get(k, k)
                    obj_class = v.pop(type(self)._OBJ_CLASS_LABEL).lower()
                    if (
                        obj_type in type(self)._OBJTYPE_CLASSMAP_MAP
                        and
                        obj_class in type(self)._OBJTYPE_CLASSMAP_MAP[obj_type]
                    ):
                        obj_params = self._parse_config_map(v)
                        v = type(self)._OBJTYPE_CLASSMAP_MAP[
                            obj_type
                        ][obj_class](**obj_params)
                else:
                    v = self._parse_config_map(v)

            # It is an object, by default
            elif isinstance(v, str):
                # NOTE: If a parameter name matches a key in
                # PARAM_OBJTYPE_MAP but it is not actually an
                # object-based parameter.
                obj_type = type(self)._PARAM_OBJTYPE_MAP.get(k, k)
                obj_class = v.lower()
                if (
                    obj_type in type(self)._OBJTYPE_CLASSMAP_MAP
                    and obj_class in type(self)._OBJTYPE_CLASSMAP_MAP[obj_type]
                ):
                    v = type(self)._OBJTYPE_CLASSMAP_MAP[obj_type][obj_class]()

            factory_config[k] = v
        return factory_config
