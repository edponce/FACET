import io
import csv
from .base import BaseFormatter


__all__ = ['CSVFormatter']


class CSVFormatter(BaseFormatter):

    NAME = 'csv'

    def _format(self, data):
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
        return fd.getvalue()
