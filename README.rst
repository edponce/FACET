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

.. figure:: doc/figures/FACET.png
   :scale: 70 %
   :alt: FACET client/server scheme

   **Ideal FACET Structure:** Schematic of FACET presenting its components and their interactions. Many components provide multiple alternatives for its processing. FACET supports the client/server paradigm, where clients ingest text streams and apply NLP to extract tokens of interest (stopwords and a parts-of-speech tagger are used to filter out “unimportant” tokens. The tokens are sent to the server which matches the tokens against a set of preinstalled canonical terms. Candidate terms can be used to extract additional attributes from preinstalled tables. The candidate terms along with their corresponding attributes are sent to the client. FACET provides multiple formats for representing the matched results.

.. figure:: doc/figures/FACET_curr.png
   :scale: 70 %
   :alt: FACET client/server scheme

   **Current FACET Architecture:** Schematic of FACET presenting its components and their interactions. Client/server communication is performed via remote procedure calls (RPC). Clients load data and send it to the server for NLP.


Setup and Installation
----------------------

1. Clone repository ::

    $ git clone https://github.com/edponce/FACET.git
    $ cd FACET/

2. Install package::

    $ pip install .

3. (Optional) Configure FACET shell command::

    $ source scripts/shell_completion.sh

4. (Optional) Install spaCy language model, see `spaCy models`_::

    $ sh scripts/setup_spacy.sh

.. _spaCy models: https://spacy.io/models/en

5. (Optional) Install NLTK NLP components::

    $ python scripts/setup_nltk.py

6. (Optional) Install UMLS files:

   You require to have a valid UMLS installation on disk. To use UMLS files,
   you must first obtain a `UMLS license`_ from the National Library of
   Medicine, then download the corresponding `UMLS`_ files. Currently, FACET in
   UMLS mode supports the *MRCONSO.RRF* and (optional) *MRSTY.RRF* files. Note
   that `UMLS`_ provides *MRCONSO.RRF* as a single downloadable item.

   For running examples, create a symbolic link of UMLS directory in FACET path::

    $ ln -s YOUR_UMLS_FULLPATH/ data/umls

.. _UMLS license: https://uts.nlm.nih.gov/license.html
.. _UMLS: https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html

7. Run tests ::

    $ tox

8. (Optional) Check command line interface ::

    $ python facet/scripts/cli.py --help
    $ python facet/scripts/cli.py run --help
    $ python facet/scripts/cli.py server --help
    $ python facet/scripts/cli.py client --help

    or (if shell_completion.sh installed)

    $ facet --help
    $ facet run --help
    $ facet server --help
    $ facet client --help


Usage
-----

FACET can be used in different modes as it functions as an application, a
command line tool, or a library.

* Run REPL on command line ::

    $ facet run --install data/install/american-english
    $ > cancer
    $ > alphametic
    $ > exit()

* Process file via command line ::

    $ facet run --install data/install/american-english --query data/sample.txt --formatter json --output annotations.json

* Run with a configuration file::

    $ facet run --config config/auto.yaml
    $ <viewer> annotations.json

* Run as a web service ::

    $ facet server --install data/install/american-english --host localhost --port 4444
    $ facet client --host localhost --port 4444 --formatter json
    $ > acetate
    $ > exit()
    $ facet server-shutdown --host localhost --port 4444

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


PERFORMANCE
===========

**Note:** We need a quality metric to compare experiments and determine good parameters.

* total N-gram count
* distribution of N-gram size
* N-grams skipped
* N-grams used
* N-grams matched
* number of documents


Installation
------------

UMLS 2018-AA with selected semantic types: 4,532,193 concepts
Semantic types: 1,782,484
Simstring (character features with n=3): 7,000,905 entries

============================ ===========
Task (in-memory dict)        Runtime (s)
============================ ===========
Load/parse semantic types    7.16
Write semantic types         1.06
Load/parse concepts          41.69
Write concepts and Simstring 615.87
Total time                   665.79
============================ ===========


Processing Throughput
---------------------

Performance of processing a collection of 100 documents (SynthNotes).  

**Bottleneck:** Profiling indicated the bottleneck was the number of calls to the Simstring database (1066595 calls to database).  

**Solution:** Use bulk database accesses. This requires extending the databases API and modifying the matching algorithm to operate on bulk operations.  

**Status:** This improvement is still under development, and preliminary results indicate that probably up to an order of magnitude in performance can be gained.


========== ============ ===================== ============== ===========
Tokenizer  Walltime (s) Throughput (doc/sec)  Single doc (s) Matches (n)
========== ============ ===================== ============== ===========
alphanum   18.6         5.3                   0.19           666803
spaCy      24.1         4.1                   0.24           715276
spaCy-n    20.5         4.8                   0.20           524782
spaCy-n/c  52.2         1.9                   0.52           337012
whitespace 15.0         6.6                   0.15           445806
NLTK       12.5         8                     0.13           340211
========== ============ ===================== ============== ===========


UMLS RELATED TOOLS
==================

* QuickUMLS: https://github.com/Georgetown-IR-Lab/QuickUMLS
* py-umls: https://github.com/chb/py-umls
* UMLS Description:

  * http://text-analytics101.rxnlp.com/2013/11/what-tui-cui-lui-you-silly-sui.html
  * https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2001-108.pdf


COMING SOON
===========

* Bulk database accesses
* New matchers string matching:

  * fuzzywuzzy
  * metaphone
