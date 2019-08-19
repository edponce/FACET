"""QuickerUMLS package.

Todo:
    Improve handling of module imports vs special attributes if using
    non-standard libraries. Currently applicable when running tox environments
    that do not include the install_requirements.txt:
        * Use try-except in __init__.py (bad hack)
        * Use explicit values in setup.py and doc/conf.py
        * Include install_requirements.txt in tox environment (e.g., doc)
"""


__title__ = "QuickerUMLS"
__name__ = "QuickerUMLS"
__version__ = "0.8"
__description__ = """High-performance tool for concept extraction
                     from medical narratives."""
__keywords__ = [
    "information extraction",
    "UMLS",
    "concepts",
    "medical text",
    "natural language processing",
]
__url__ = "code.ornl.gov:REACHVET/reachvet-nlp/quicker-umls.git"
__author__ = "Eduardo Ponce, Oak Ridge National Laboratory, Oak Ridge, TN"
__author_email__ = "poncemojicae@ornl.gov"
__license__ = "MIT"
__copyright__ = """2019 Eduardo Ponce and Kris Brown
                   Oak Ridge National Laboratory"""


# __all__ = ()


from .quickumls import QuickUMLS
from .database.redis import RedisDatabase
from .database.dict import DictDatabase
from .simstring import simstring
