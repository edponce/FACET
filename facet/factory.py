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
)


__all__ = ['FacetFactory']


facet_map = {
    'Facet': Facet,
    'UMLSFacet': UMLSFacet,
}


SETTING_MAP = {
    'tokenizer': tokenizer_map,
    'formatter': formatter_map,
    'database': database_map,
    'simstring': simstring_map,
    'similarity': similarity_map,
    'ngram': ngram_map,
    'serializer': serializer_map,
}

CLASS_LABEL = 'class'
OBJ_TYPE_LABEL = 'class'


class FacetFactory:
    """Generate a FACET instance from a given configuration."""

    def __init__(self, config: str, *, key: str = None):
        _config = load_configuration(config, keys=key)
        if key is None and ':' not in config:
            if _config:
                if len(_config) > 1:
                    raise ValueError('configuration has too many sections, '
                                     'specify key or use single-section')

                _config = _config[list(_config.keys())[0]]

        factory_settings = self._facet(_config)
        if _config[CLASS_LABEL] == 'UMLSFacet':
            factory_settings.update(self._umlsfacet(_config))

        self._config = _config
        self._factory_settings = factory_settings

    def create(self):
        return facet_map[self._config[CLASS_LABEL]](**self._factory_settings)

    __call__ = create

    def _resolve_setting(self, key, config: Dict[str, Any], *, map_key=None):
        # This method performs operation similar to:
        #
        # if 'cuisty_db' in config:
        #     cls = config['cuisty_db'].pop('class')
        #     db = database_map[cls.lower()](**config['cuisty_db'])
        #     factory_settings['cuisty_db'] = db

        setting = {}
        if key in config:
            _config = config[key]

            # NOTE: Only resolve parameters that support objects, either via
            # a map or an instance (or None).
            if _config is None:
                tok = None
            elif isinstance(_config, str):
                tok = _config.lower()
            elif isinstance(_config, dict):
                cls = _config.pop(OBJ_TYPE_LABEL)
                map_key = key if map_key is None else map_key
                tok = SETTING_MAP[map_key][cls.lower()](**_config)
            else:
                raise ValueError(f'invalid configuration type, {_config}')
            setting[key] = tok
        return setting

    def _facet(self, config: Dict[str, Any]) -> Dict[str, Any]:
        factory_settings = {}
        if 'simstring' in config:
            for key in ('db', 'cache_db'):
                config['simstring'].update(self._resolve_setting(
                    key,
                    config['simstring'],
                    map_key='database',
                ))
            for key in ('similarity', 'ngram'):
                config['simstring'].update(self._resolve_setting(
                    key,
                    config['simstring'],
                ))

        for key in ('simstring', 'tokenizer', 'formatter'):
            factory_settings.update(self._resolve_setting(key, config))
        return factory_settings

    def _umlsfacet(self, config: Dict[str, Any]) -> Dict[str, Any]:
        factory_settings = {}
        for key in ('conso_db', 'cuisty_db'):
            if key in config:
                config[key].update(self._resolve_setting(
                    'serializer',
                    config[key],
                ))
            factory_settings.update(self._resolve_setting(
                key,
                config,
                map_key='database',
            ))
        return factory_settings
