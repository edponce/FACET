import copy
from .facets import facet_map
from .formatter import formatter_map
from .tokenizer import tokenizer_map
from .database import database_map
from .serializer import serializer_map
from .matcher import matcher_map
from .matcher.similarity import similarity_map
from .matcher.ngram import ngram_map
from .configuration import Configuration
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
        self._config = type(self)._load_config(config, section=section)

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
        return facet_map[obj_class](**type(self)._parse_config_map(config))

    __call__ = create

    @classmethod
    def _load_config(
        cls,
        config: Union[str, Dict[str, Any]] = None,
        *,
        section: str = None,
    ):
        _config = Configuration().load(config, keys=section)
        # NOTE: Remove CLI-specific parameters
        for key in cls._INVALID_KEYS:
            if key in _config:
                del _config[key]
        return _config

    @classmethod
    def create_generic(
        cls,
        config: Union[str, Dict[str, Any]] = None,
        *,
        section: str = None,
    ) -> Any:
        """Create a new instance of any FACET-related classes."""
        # NOTE: Copy configuration because '_parse_config_map()' modifies it.
        _config = copy.deepcopy(cls._load_config(config, section=section))
        objs = [v for k, v in cls._parse_config_map(_config).items()]
        return objs if len(objs) > 1 else objs[0]

    @classmethod
    def _parse_config_map(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """Recusively parse a configuration map."""
        factory_config = {}
        for k, v in config.items():
            # It is an object, with class and parameters
            if isinstance(v, dict):
                # If parameter is a dictionary representing an object.
                if cls._OBJ_CLASS_LABEL in v:
                    # NOTE: If a parameter name matches a key in
                    # PARAM_OBJTYPE_MAP but it is not actually an
                    # object-based parameter.
                    obj_type = cls._PARAM_OBJTYPE_MAP.get(k, k)
                    obj_class = v.pop(cls._OBJ_CLASS_LABEL).lower()
                    if (
                        obj_type in cls._OBJTYPE_CLASSMAP_MAP
                        and
                        obj_class in cls._OBJTYPE_CLASSMAP_MAP[obj_type]
                    ):
                        obj_params = cls._parse_config_map(v)
                        v = cls._OBJTYPE_CLASSMAP_MAP[
                            obj_type
                        ][obj_class](**obj_params)
                else:
                    v = cls._parse_config_map(v)

            # It is an object, by default
            elif isinstance(v, str):
                # NOTE: If a parameter name matches a key in
                # PARAM_OBJTYPE_MAP but it is not actually an
                # object-based parameter.
                obj_type = cls._PARAM_OBJTYPE_MAP.get(k, k)
                obj_class = v.lower()
                if (
                    obj_type in cls._OBJTYPE_CLASSMAP_MAP
                    and obj_class in cls._OBJTYPE_CLASSMAP_MAP[obj_type]
                ):
                    v = cls._OBJTYPE_CLASSMAP_MAP[obj_type][obj_class]()

            factory_config[k] = v
        return factory_config
