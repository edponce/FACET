import io
import csv
import json
import yaml
import pickle
import dicttoxml
import xml.dom.minidom
from typing import Any, List, Dict


__all__ = ['Formatter']


class Formatter:
    """Format data and write to stream."""

    def __init__(self, format=None):
        self._format = None
        self.format = format

    @property
    def format(self) -> str:
        return self._format

    @format.setter
    def format(self, format: str):
        if format is not None:
            format = format.lower()
            if format not in ('json', 'yaml', 'xml', 'pickle', 'csv'):
                raise ValueError(
                    f'Error: invalid format option, {format} is not supported'
                )
        self._format = format

    def __call__(
        self,
        data: Dict[str, List[List[Dict[str, Any]]]],
        *,
        format: str = '',
        outfile: str = None,
    ) -> Any:
        """
        Args:
            data (Dict[str, List[List[Dict[str, Any]]]]): Mapping of data and
                attributes.

            format (str): Formatting mode for match results. Valid values
                are: 'json', 'yaml', 'xml', 'pickle', 'csv'. Default is None.
        """
        if format == '':
            format = self._format

        # NOTE: Some of the formatters support writing to a file stream
        # directly. This is an alternative instead of generating in-memory
        # string and then writing to file.
        if format is None or format == 'none':
            formatted_data = data
        elif format == 'json':
            formatted_data = json.dumps(data, indent=2)
        elif format == 'yaml':
            formatted_data = yaml.dump(data)
        elif format == 'xml':
            formatted_data = xml.dom.minidom.parseString(
                dicttoxml.dicttoxml(data, attr_type=False)
            ).toprettyxml(indent=2 * ' ')
        elif format == 'pickle':
            formatted_data = pickle.dumps(data)
        elif format == 'csv':
            # Find field names
            fieldnames = []
            for _, v in data.items():
                if len(v) > 0 and len(v[0]) > 0:
                    fieldnames = ['source'] + list(v[0][0].keys())
                    break
            # Write to in-memory stream
            fd = io.StringIO()
            writer = csv.DictWriter(fd, fieldnames, lineterminator='\n')
            writer.writeheader()
            for src, v1 in data.items():
                for v2 in v1:
                    for v3 in v2:
                        v3['source'] = src
                    writer.writerows(v2)
            formatted_data = fd.getvalue()
        else:
            raise ValueError(f'invalid format value, {format}')

        if outfile is not None:
            with open(outfile, 'w') as fd:
                # NOTE: Explicit conversion to string, if format is None we
                # want to return the data as is and be able to write to file.
                fd.write(str(formatted_data))

        return formatted_data
