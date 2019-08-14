from annotated_document.generic import AnnotatedDocument


class AnnotatedDocuments:
    '''Container for annotated documents.

    An :class:`AnnotatedDocuments` object is immutable.
    '''

    def __init__(self, category=''):
        self._category = category
        self._documents = set()  # mutable, no duplicate, no index, no order

    def __repr__(self):
        return f"{type(self).__qualname__}("\
               f"category={self._category}, "\
               f"num_documents={len(self)}"\
               f")"

    def __str__(self):
        outstr = f"category={self.category}\n"\
                 f"num_documents={len(self)}\n"
        for d in self.documents:
            outstr += str(d) + '\n'
        return outstr

    def __len__(self):
        return len(self.documents)

    @property
    def category(self):
        return self._category

    @property
    def documents(self):
        return self._documents

    @property
    def size(self):
        return len(self)

    def add(self, document):
        type(self)._validate_document(document)
        self._documents.add(document)

    def __iter__(self):
        yield from self.documents

    def extract_annotations(self, category=''):
        for d in self.documents:
            d.extract_annotations(category)

    @staticmethod
    def _validate_document(document):
        if not isinstance(document, AnnotatedDocument):
            raise TypeError(f"not a valid {AnnotatedDocument.__qualname__} object")
