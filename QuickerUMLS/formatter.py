import io
import csv
import json
import pickle
import dicttoxml
import xml.dom.minidom
from typing import Any, List, Dict


__all__ = ['Formatter']


class Formatter:
    """
    Args:
        format (str): Output format.
            Valid values are 'json', 'xml', 'pickle', 'csv', and None.
            Default is None.

        outfile (str): Output file. Default is None (print to stdout).
    """

    def __init__(self, format: str = None, *, outfile: str = None):
        self._format = None
        self._outfile = None
        self.format = format
        self.outfile = outfile

    @property
    def format(self) -> str:
        return self._format

    @format.setter
    def format(self, format: str):
        if format is not None:
            format = format.lower()
            if format not in ('json', 'xml', 'pickle', 'csv'):
                raise ValueError(
                    f'Error: invalid formatting, {format} is not supported'
                )
        self._format = format

    @property
    def outfile(self) -> str:
        return self._outfile

    @outfile.setter
    def outfile(self, outfile: str):
        self._outfile = outfile

    def __call__(self, data: Dict[str, List[List[Dict[str, Any]]]]) -> Any:
        """
        Args:
            data (Dict[str, List[List[Dict[str, Any]]]]): Mapping of data and
                attributes.
        """
        if self._format is None:
            formatted_data = data
        elif self._format == 'json':
            formatted_data = json.JSONEncoder(indent=2).encode(data)
        elif self._format == 'xml':
            formatted_data = xml.dom.minidom.parseString(
                dicttoxml.dicttoxml(data, attr_type=False)
            ).toprettyxml(indent=2 * ' ')
        elif self._format == 'pickle':
            formatted_data = pickle.dumps(data)
        elif self._format == 'csv':
            # Find fieldnames
            fieldnames = []
            for k, v in data.items():
                if len(v) > 0 and len(v[0]) > 0:
                    fieldnames = list(v[0][0].keys())
                    break
            # Write to in-memory stream
            fd = io.StringIO()
            writer = csv.DictWriter(fd, fieldnames, lineterminator='\n')
            writer.writeheader()
            for k1, v1 in data.items():
                for v2 in v1:
                    writer.writerows(v2)
            formatted_data = fd.getvalue()

        if self._outfile is not None:
            with open(self._outfile, 'w') as fd:
                fd.write(formatted_data)
        else:
            return formatted_data
