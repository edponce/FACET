try:
    # Use libmagic for encoding detection, pip install python-magic
    import magic
    MAGIC_AVAILABLE = True
except:
    MAGIC_AVAILABLE = False


class Document:
    '''Representation of text data.

    A :class:`Document` object stores text data as a string and properties associated with it. The text data is independent of its source or destination.

    Note:

        * Document ID is an optional and arbitrary typed field. Commonly it is either a string or numeric value that is unique in the application.
        * Document source is used to represent the place of origin of the text data. Possible values are: filename, database-table name, dataset name, etc.
        * Allow copying documents, but how should document ID be handled?
        * When should text size be calculated? Needs to be consistent with text data.
    '''

    def __init__(self, **kwargs):
        doc = kwargs.get('document')
        if doc:
            # If ID is explicitly provided, use it
            self.identifier = kwargs.get('identifier', doc.identifier)
            self.source = doc.source
            self.size = doc.size
            self.encoding = doc.encoding

            # Strings in Python are immutable, so get a reference not a copy
            self.text = doc.text
        else:
            self.identifier = kwargs.get('identifier', None)
            self.source = kwargs.get('source', None)
            self.size  = kwargs.get('size', 0)
            self.encoding = kwargs.get('encoding', None)
            self.text = kwargs.get('text', None)

    def __repr__(self):
        # TODO: attributes that are strings should be placed within quotes,
        #       but sometimes they have None values (e.g., encoding)
        return f"{type(self).__qualname__}("\
               f"identifier={self.identifier}, "\
               f"source={self.source}, "\
               f"size={self.size}, "\
               f"encoding={self.encoding}"\
               f")"

    def __str__(self):
        return f"identifier={self.identifier}\n"\
               f"source={self.source}\n"\
               f"size={self.size}\n"\
               f"encoding={self.encoding}\n"\
               f"text={self.text}"

    def updateSize(self):
        self.size = len(self.text)

    def updateEncoding(self):
        if MAGIC_AVAILABLE:
            self.encoding = magic.Magic(mime_encoding=True).from_buffer(self.text)
        else:
            self.encoding = None
