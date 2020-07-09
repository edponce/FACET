.. .. image:: https://codecov.io/gh/kbrown42/quickerumls/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/edponce/quickerumls
   :alt: Coverage Status

.. .. image:: https://readthedocs.org/projects/quickerumls/badge/?version=latest
   :target: https://quickerumls.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. .. image:: https://img.shields.io/badge/license-MIT-blue.svg
   :target: https://github.com/edponce/smarttimers/blob/master/LICENSE
   :alt: License

|

FACET - Framework for Annotation and Concept Extraction in Text
===============================================================

FACET is an extension of `QuickUMLS`_ and `SimstringPure`_ tools providing
faster, scalable, and flexible concept extraction from medical narratives.
Uses the simple and efficient CP-Merge approach of `Simstring`_ algorithm.

.. _QuickUMLS: https://github.com/Georgetown-IR-Lab/QuickUMLS
.. _SimstringPure: https://pypi.org/project/simstring-pure
.. _Simstring: http://www.chokkan.org/software/simstring


Features
--------

* Simple configuration and easy to use
* Multiple modes of operation
* Multiple database backends
* Multiprocessing for processing corpi
* Easy to scale up, run locally, or containerize
* Stores extracted annotations in serializable and/or human-readable formats


Setup and Installation
----------------------

1. Clone repository ::

    $ git clone https://github.com/edponce/FACET.git
    $ cd FACET/

2. Install package ::

    $ conda env create --file environment.yaml
    $ conda activate facet

    or

    $ pip install .[extra]

2a. (Optional) Install spaCy language support::

    $ python -m spacy download en

2b. (Optional) Install NLTK NLP components::

    $ python scripts/setup_nltk.py

    or

    >>> import nltk
    >>> nltk.download('stopwords')
    >>> nltk.download('punkt')
    >>> nltk.download('averaged_perceptron_tagger')
    >>> nltk.download('universal')

2c. (Optional) Install UMLS files::

   You require to have a valid UMLS installation on disk. To use UMLS files, you
   must first obtain a `UMLS license`_ from the National Library of Medicine,
   then download the corresponding `UMLS`_ files. Currently, FACET in UMLS mode
   supports the *MRCONSO.RRF* and (optional) *MRSTY.RRF* files. Note that `UMLS`_
   provides *MRCONSO.RRF* as a single downloadable item.

.. _UMLS license: https://uts.nlm.nih.gov/license.html
.. _UMLS: https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html

   For running examples, create a symbolic link of UMLS directory in FACET path.

   $ ln -s YOUR_UMLS_FULLPATH/ data/umls

3. Run tests ::

    $ tox

4. Generate documentation ::

    $ make -C doc/ html
    $ <browser> doc/_build/html/index.html

5. Browse commands and help descriptions ::

    $ facet --help


Usage
-----

FACET can be used in different modes as it functions as an application, a
command line tool, or a library.

* Run REPL on command line ::

    $ facet run --install data/sample.txt
    $ > cancer
    $ > thorax
    $ > alpha()              # Get similarity threshold
    $ > alpha = 0.5          # Set similarity threshold
    $ > similarity()         # Get similarity metric
    $ > similarity = cosine  # Set similarity metric
    $ > formatter()          # Get format mode
    $ > formatter = json     # Set format mode
    $ > exit()               # Stop FACET

* Process file via command line ::

    $ facet run --database redis --query corpus.txt --format json --output corpus.json

* Run with a configuration file::

    $ facet run --config config/factory.yaml:SimpleMemory --output corpus.json

* Run as a web service ::

    $ facet server --port 4452
    $ facet client --host localhost --port 4452 --format json

* Run programmatically using Python's API (see example scripts) ::

    $ <viewer> examples/install.py
    $ <viewer> examples/match.py


Databases Initialization
------------------------

FACET supports the following databases for backend storage, and due to its modular
structure different database types can be used in the same installation.

* Python dictionary (in-memory) - fast performance, but increases main process storage and does not persists after system shutdown
* Python dictionary (file backed) - fast performance, but increases main process storage. Persists after system shutdown.
* Python SQLite3 (in-memory, file backed) - medium performance
* Redis - medium performance.


Redis
^^^^^
* Install Redis server/client packages in computer system::

  $ apt install redis-server


Performance
===========

============================ ===========
Task (dict)                  Runtime (s)
============================ ===========
Load concepts                5.48e-05
Write concepts and Simstring 59.00
Load semantic types          2.28e-05
Write semantic types         10.49
============================ ===========

============================ ===========
Task (Redis)                 Runtime (s)
============================ ===========
Load concepts                5.76e-05
Write concepts and Simstring 816.18
Load semantic types          2.38e-05
Write semantic types         153.19
============================ ===========


UMLS Related Tools
==================

py-umls: https://github.com/chb/py-umls
UMLS Description:
* http://text-analytics101.rxnlp.com/2013/11/what-tui-cui-lui-you-silly-sui.html
* https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2001-108.pdf


Coming Soon
===========

Matchers with fuzzy string matching:
* ElasticSearch
* fuzzywuzzy
* python-Levenshtein
