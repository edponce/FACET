import sys
sys.path.append('./data_models')
from .document.generic import Document
from .document.container import Documents
from .annotation.generic import Annotation
from .annotation.container import Annotations
from .annotated_document.generic import AnnotatedDocument
from .annotated_document.container import AnnotatedDocuments
from .text_source import TextSource
