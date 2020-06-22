import dicttoxml
import xml.dom.minidom
from .base import BaseFormatter


__all__ = ['XMLFormatter']


class XMLFormatter(BaseFormatter):

    def _format(self, data):
        return xml.dom.minidom.parseString(
            dicttoxml.dicttoxml(data, attr_type=False)
        ).toprettyxml(indent='  ')
