from annotation.generic import Annotation
from annotation.container import Annotations


class AnnotatedDocument:
    '''Representation of an annotated text data.
    '''

    def __init__(self, document):
        self._document = document
        self._annotations = {}

    def __str__(self):
        outstr = str(self.document) + '\n'
        for cat, a in self.annotations.items():
            outstr += str(a) + '\n'
        return outstr

    @property
    def document(self):
        return self._document

    @property
    def annotations(self):
        return self._annotations

    def extract_annotations(self, category=''):
        if category is None:
            raise NotImplementedError('extract annotations from all categories')
        elif category in self.annotations:
            annotations = Annotations(category=category)
            for a in self.annotations[category]:
                text = self.document.text[a.begin:a.end]
                annotation = Annotation(begin=a.begin, end=a.end, text=text)
                annotations.add(annotation)
            self._annotations[category] = annotations

    def add_annotations(self, annotations):
        if annotations.category in self.annotations:
            for a in annotations:
                self._annotations[annotations.category].add(a)
        else:
            self._annotations[annotations.category] = annotations
