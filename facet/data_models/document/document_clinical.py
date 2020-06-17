from document import Document
from annotation_clinical import NLPAnnotationClinical


class Clinical_Document(Document):
    '''Representation of clinical text data.
    '''

    def __init__(self, **kwargs):
        self.NLPannotation = NLPAnnotationClinical()
