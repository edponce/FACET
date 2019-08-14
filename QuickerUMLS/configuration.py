import os
from ruamel import yaml
import json
import copy


class Configuration:
    """ Represents an extensible and generic configuration.
    Contains methods to load, set, and query configuration properties.
    A configuration load supports dictionary and string or file in
    YAML/JSON formats.
    """

    def __init__(self, conf=None):
        # File formats/extensions supported (default is set first).
        self.__file_formats = ('yaml', 'yml', 'json')

        # Functions that take dictionary, string, or file and return a
        # dictionary, keyed by file extension.
        # Interface is of the form load(data).
        self.__loaders = {'yaml': self.__load_yaml,
                          'yml':  self.__load_yaml,
                          'json': self.__load_json}

        # Functions that take a dictionary and write to file, keyed
        # by file extension.
        # Interface is of the form dump(data, stream).
        self.__dumpers = {'yaml': yaml.dump,
                          'yml':  yaml.dump,
                          'json': json.dump}

        # Configuration dictionary.
        self.__config = {}

        if conf is not None:
            self.load(conf)

    def __load_yaml(self, file):
        """ Loads data from a YAML string or file into a dictionary. """
        def _load_yaml(f):
            data = {}
            try:
                data = yaml.safe_load(f)
            except yaml.YAMLError as ex:
                print(f'Failed to parse YAML data: {ex}')
            return data

        data = {}
        if os.path.isfile(file):
            try:
                with open(file, 'r') as fd:
                    data = _load_yaml(fd)
            except Exception as ex:
                print(f'Failed to open YAML file ({file}): {ex}')
        else:
            data = _load_yaml(file)
        return data

    def __load_json(self, file):
        """ Loads data from a JSON string or file into a dictionary. """
        def _load_json(f):
            data = {}
            try:
                data = json.load(f)
            except json.JSONDecodeError as ex:
                print(f'Failed to parse JSON data: {ex}')
            return data

        data = {}
        if os.path.isfile(file):
            try:
                with open(file, 'r') as fd:
                    data = _load_json(fd)
            except Exception as ex:
                print(f'Failed to open JSON file: {ex}')
        else:
            data = _load_json(file)
        return data

    def load(self, conf):
        """ Load a configuration and overwrite entire configuration. """
        data = {}
        if isinstance(conf, dict):
            data = conf
        elif isinstance(conf, str):
            if os.path.isfile(conf):
                _, ext = os.path.splitext(conf)
                ext = ext[1:].lower()
                data = self.__loaders[ext](conf)
            else:
                for loader in self.__file_formats:
                    try:
                        data = self.__loaders[loader](conf)
                        break
                    except Exception as ex:
                        pass
        self.__config = data

    def dump(self, file):
        _, ext = os.path.splitext(file)
        ext = ext[1:].lower()
        if ext not in self.__file_formats:
            ext = self.__file_formats[0]
        with open(file, 'wb') as fd:
            self.__dumpers[ext](self.__config, fd)

    def update(self, conf):
        """ Updates an existing configuration by replacing and adding
        new key/value pairs dictionary. """
        pass

    def get(self, key=None):
        """ Queries configuration properties. """
        if key is None:
            return copy.deepcopy(self.__config)
        else:
            return self.__config.get(key, {})

    def set(self, key, value):
        """ Queries configuration properties. """
        if key in self.__config:
            self.__config[key] = value

    def __str__(self):
        return str(self.__config)
