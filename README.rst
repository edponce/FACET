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

* Easy install process
* Utilizes Redis for faster UMLS key lookup
* Adds multiprocessing for:

  * Processing batches of notes
  * Process batches of n-grams from each note

* Stores extracted annotations in an easy to analyze format

  * Should this be OMOP ID’d annotations?

* Can evaluate the quality of extracted terms for accuracy based on publicly available medical data sets
* Can evaluate the quality of extracted terms for accuracy based on comparison with an equivalent cTAKES pipeline
* New features are fully tested. Old features as well
* Integrated into a continuous-integration pipeline system in version control
* First release will have a demo with VA. So this needs to have a clean API, impressive performance, and demonstrable value over other tools.

  * Easy config
  * Easy to use programmatically
  * Easy to scale up or run locally


Installation
------------

#. Install Python3 development headers for host system

    > apt install python3-dev

#. You can install requirements manually, pip install -r requirements.
#. In order to use spaCy, download the relevant corpus, python3 -m spacy download en.
#. In order to use NLTK, download stopwords

    >>> import nltk
    >>> nltk.download('stopwords')
    >>> nltk.download('punkt')
    >>> nltk.download('averaged_perceptron_tagger')
    >>> nltk.download('universal')

#. You require to have a valid UMLS installation on disk. To install UMLS, you
   must first obtain a `UMLS license`_ from the National Library of Medicine,
   then download all `UMLS files`_. Finally, you can install UMLS using the
   `MetamorphoSys`_ tool. The installation can be removed once the system has
   been initialized.


.. _UMLS license: https://uts.nlm.nih.gov/license.html
.. _UMLS files: https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html
.. _MetamorphoSys: https://www.nlm.nih.gov/research/umls/implementation_resources/metamorphosys/help.html


Setup and Installation
----------------------

1. Clone repository ::

    $ git clone https://github.com/edponce/FACET.git
    $ cd FACET/

2. Install package ::

    $ pip install .

3. Run tests ::

    $ tox

4. Generate documentation ::

    $ make -C doc/ html
    $ <browser> doc/_build/html/index.html

5. Browse commands and help descriptions ::

    $ facet --help

6. Run demo ::

    $ python examples/demo_install.py
    $ python examples/demo_match.py


Usage
-----

* Run REPL on command line ::

    $ facet --cli
    $ > cancer

* Process file via command line ::

    $ facet --format json --infile corpus.txt --outfile corpus.json

* Process text via STDIN ::

    $ cat corpus.txt | facet --pipe --format json --outfile corpus.json

* Run as a web service ::

    $ facet --port 4452 --format json

* Run programmatically using Python's API (see example scripts) ::

    $ <editor> examples/demo_match.py


Databases Initialization
------------------------

FACET supports the following databases for backend storage. Note that different
databases can be used in the same installation.

* Redis - requires DSN information.
* Python dictionary (in-memory) - fast performance, but increases main process storage and does not persists after system shutdown.
* Python dictionary (file backed) - fast performance, but increases main process storage. Persists after system shutdown.

Create databases for data (UMLS MRCONSO and MRSTY) and Simstring. This process takes approximately 30 minutes.

    >>> python3 facet/install.py --lowercase --normalize-unicode --umls-dir /path/to/UMLS/RRF/files --install-dir /path/to/install/UMLS/database
    >>> python3 facet/install.py -l -n -u /path/to/UMLS/RRF/files -i /path/to/database/installation


-u dir, --umls-dir=dir  Directory of the UMLS installation (in particular, we need MRCONSO.RRF and MRSTY.RRF).
-i dir, --install-dir=dir  Directory where the QuickUMLS data files will be installed.
-L, --lowercase  Fold all concept terms to lowercase before being processed. This option typically increases recall, but it might reduce precision.
-U, --normalize-unicode  Expressions with non-ASCII characters are converted to the closest combination of ASCII characters.
-E, --language  Specify the language to consider for UMLS concepts (defuault is English). For a complete list of languages, see `NLM language table`_.


.. _NLM language table: https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/abbreviations.html#LAT

The following are results for a subset of UMLS 2018-AA:
200,110 concepts, 1,783,491 CUIs (all MRSTY.RRF).

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

The following are results for a subset of UMLS 2018-AA:
20,347 concepts, 943 CUIs (filtered MRSTY.RRF).

============================ ===========
Task (2 Redis/Elasticsearch) Runtime (s)
============================ ===========
Load concepts (iter_data)    8.77e-05
Write concepts and Simstring 309.36
Load semantic types          5.05e-05
Write semantic types         0.20
============================ ===========

============================ ===========
Task (2 Redis/Elasticsearch) Runtime (s)
============================ ===========
Load concepts (data_to_dict) 0.49
Write concepts and Simstring 314.95
Load semantic types          0.17
Write semantic types         0.11
============================ ===========

The following are results for a subset of UMLS 2018-AA:
20,269 concepts, 31154 CUIs using bulk API.
Redis 2.3 MB (incorrect b/c other tables were populated)
Elasticsearch 14 MB

============================ ===========
Task (2 Redis/Elasticsearch) Runtime (s)
============================ ===========
Load concepts (data_to_dict) 0.30
Write concepts               0.24
Write Simstring              2.13
Load semantic types          0.19
Write semantic types         0.34
============================ ===========

The following are results for a subset of UMLS 2018-AA:
198,696 concepts, 1,782,484 CUIs using bulk API.
Redis 41 MB (incorrect b/c other tables were populated)
Elasticsearch 79 MB

============================ ===========
Task (2 Redis/Elasticsearch) Runtime (s)
============================ ===========
Load concepts (data_to_dict) 2.82
Write concepts               2.24
Write Simstring              22.75
Load semantic types          6.90
Write semantic types         19.19
============================ ===========

The following are results for a subset of UMLS 2018-AA:
198,696 concepts, 9,518 CUIs using bulk API.

============================ ===========
Task (2 Redis/Elasticsearch) Runtime (s)
============================ ===========
Load concepts (data_to_dict) 3.10
Write concepts               2.18
Write Simstring              23.79
Load semantic types          4.32
Write semantic types         0.10
============================ ===========

The following are results for full UMLS 2018-AA:
8,015,988 concepts, ? CUIs (all MRSTY.RRF).

====================  ===========  ====================
Task                  Runtime (s)  Comments
====================  ===========  ====================
Load concepts         0.0000205    File (pandas)
Write concepts        247.99       Level DB
Write Simstring DB    444.90       Files (Simstring DB)
Load semantic types   0.02         File (pandas)
Write semantic types  10.10        Level DB
Total install         736.32
====================  ===========  ====================


API and Usage
-------------

QuickUMLS(quickumls_fp, overlapping_criteria, threshold, similarity_name, window, accepted_semtypes):

* quickumls_fp is the directory for the UMLS installation
* overlapping_criteria (optional, default="score") is the criteria used to deal
  with overlapping concepts; choose "score" if the matching score of the concepts
  should be consider first, "length" if the longest should be considered first
  instead.
* threshold (optional, default: 0.7) is the minimum similarity value between strings.
* similarity_name (optional, default: "jaccard") is the name of similarity to use.
  Choose between "dice", "jaccard", "cosine", or "overlap".
* window (optional, default: 5) is the maximum number of tokens to consider for
  matching.
* accepted_semtypes (optional, default: see constants.py) is the set of UMLS
  semantic types concepts should belong to. Semantic types are identified by the
  letter "T" followed by three numbers (e.g., "T131", which identifies the
  type of `Hazardous or Poisonous Substance`_).

Instantiate a QuickUMLS object:
    >>> matcher = QuickUMLS('path/to/database/installation')

NOTE: This command will invoke NLTK which in turn downloads a package of stopwords
which are placed in the home directory. For English language there 179 stopwords.

Use the QuickUMLS object:
    >>> text = "The ulna has dislocated posteriorly from the trochlea of the humerus."
    >>> matches = matcher.match(text, best_match=True, ignore_syntax=False)
    >>> matches
    >>> [[{'start': 61, 'end':68, 'ngram': 'humerus', 'term': 'humerus', 'cui': 'C0020164', 'similarity': 1.0, 'semtypes': {'T023'}, 'preferred': 1}], [...]]

Set 'best_match' to 'False' if you want to return overlapping candidates.
Set 'ignore_syntax' to 'True' to disable all heuristics introduced in Soldaini
and Goharian 2016.

.. _Hazardous or Poisonous Substance: https://metamap.nlm.nih.gov/Docs/SemanticTypes_2018AB.txt


BENCHMARKS
==========

Tests were done using UMLS 2018-AA knowledge base.
Number of workers is None (number of cores or more).
Input text is processed as a single string passed to match().

=======  ==========  ===========  ===========  ===============  ============  =====
Version  Num ws/ngs  Nmatch/best  make_ngrams  get_all_matches  select_terms  Total
=======  ==========  ===========  ===========  ===============  ============  =====
orig     2248/5528   305/250      8e-06        0.63             0.009         0.89
gam_p1                                         2.2              0.02          2.5
gam_p2                                         0.8              0.02          1.2
gam_p2b                                        0.7              0.01          1.0
gam_p3                                         1.2              0.03          1.4
gam_p4b                                        0.66             0.008         0.9
mn_2                              7e-06
mn_p1b                            0.1          0.43             0.008         0.75
mn_p2b                            0.1          0.42             0.008         0.74
merg_b                                         0.72             0.009         0.92
orig     2248/8881   525/252      3e-06 (mts)  0.95             0.012         1.2
mts_p1b                           0.1          1.0              0.012         1.3
mts_p2b                           0.1          1.0              0.012         1.3
=======  ==========  ===========  ===========  ===============  ============  =====

Real values represent time in seconds.

Legend:

* orig - original code
* gam_pX - get_all_matches_parX
* gam_pXb - get_all_matches_parX_batch
* mn_X - make_ngramsX
* mn_pXb - make_ngrams_parX_batch
* mts - uses make_token_sequence instead of make_ngrams
* mts_pXb - make_token_sequence_par2_batch
* merg_b - merge make_ngrams and get_all_matches using batches


* get_all_matches_par1 - uses concurrent.futures.ThreadPoolExecutor distributing one data at a time. Checks if partial results are None, then combines with the final result.
* get_all_matches_par2 - uses multiprocessing.pool.ThreadPool with single blocking map, then applies filter for ignoring Nones. Converts final results to a list.
* get_all_matches_par2_batch - uses multiprocessing.pool.ThreadPool with multiple apply_async that operate on batches (1024) of data. Partial results are combined into the final result.
* get_all_matches_par3 - uses multiprocessing.pool.ThreadPool with multiple apply_async that operate on single data at a time. Checks if partial results are None, then combines with the final result.
* get_all_matches_par4_batch - uses threading.Thread to spawn multiple threads that operate on batches (1024) of data. Each thread adds partial results to a shared final result.
* make_ngrams2 - removes lists used for identifying spans to ignore, etc. Performs those checks as data is processed.
* make_ngrams_par1_batch - uses multiprocessing.pool.ThreadPool with multiple apply_async that operate on batches (64) of data. Partial results from generators are combined into the final result.
* make_ngrams_par2_batch - uses multiprocessing.pool.ThreadPool with multiple apply_async that operate on batches (64) of data. Partial results from list are combined into the final result.
* make_token_sequences_par1_batch - uses multiprocessing.pool.ThreadPool with multiple apply_async that operate on batches (64) of data. Partial results from generators are combined into the final result.
* make_token_sequences_par2_batch - uses multiprocessing.pool.ThreadPool with multiple apply_async that operate on batches (64) of data. Partial results from list are combined into the final result.


Redis
=====

Redis database perform queries using a single-thread at a time (lock).

* Install Redis server/client packages in computer system (e.g., apt install redis-server).
* Install redis-py Python package (pip install redis).


Plyvel and LevelDB
==================

Using plyvel (https://github.com/wbolster/plyvel) interface for LevelDB (https://github.com/google/leveldb).

LevelDB Features:

* Keys and values are arbitrary byte arrays.
* Data is stored sorted by key.
* Basic operations: Put(key, value), Get(key), Delete(key).
* Multiple changes can be made in one atomic batch.
* Forward and backward iteration is supported over the data.
* Data is automatically compressed (Snappy compression library).

LevelDB Limitations:

* Only a single process (possibly multi-threaded) can access a particular database at a time.

  * plyvel._plyvel.IOError: b'IO error: lock test.db/LOCK: Resource temporarily unavailable'

Plyvel Info:

* Uses Cython, can be installed manually on system (repo contains Dockerfile). This might be good to increase performance for the target architecture.


Plyvel API:

* close() - closing the database while other threads are busy accessing it may result in hard crashes. Applications should make sure not to close databases that are concurrently used from other threads.
* write_batch(transaction=False, sync=False) - create a WriteBatch instance for this database.

  * transaction - whether to enable transaction-like behaviour when used in 'with' block.
  * sync - whether to use synchronous writes

* class WriteBatch - batch put/delete operations. Instances of this class can be used as context managers, when the 'with' block terminates, the batch will be automatically written to the database without an explicit call to 'WriteBatch.write()'.

    >>> with db.write_batch() as b:
    >>> b.put(b'key', b'value')


spaCy
=====

spaCy has limits into the size of text processed:
    >>> import spacy
    >>> nlp = spacy.load('en')
    >>> doc = nlp('very long text ...')
    >>> ValueError: [E088] Text of length 1639120 exceeds maximum of 1000000. The v2.x parser and NER models require roughly 1GB of temporary memory per 100,000 characters in the input. This means long texts may cause memory allocation errors. If you're not using the parser or NER, it's probably safe to increase the `nlp.max_length` limit. The limit is in number of characters, so you can check whether your inputs are too long by checking `len(text)`.


TODO
====

* Fix setup.py, __init__, modules, etc.  * Use multiprocessing in method "_get_all_matches" from "quickumls.py"
* QuickUMLS uses CPMerge, see following papers

  * 2016boguszewski
  * 2010okazaki

* Code follows the open/close principle
* The input text should not contain encoded symbols as FACET assumes ASCII/UTF-8
  text. To decode HTML characters, use

    >>> import html
    >>> html.unescape('a&#32;space')


Multiple Redis Instances
------------------------

If multiple databases are required, it is recommended to run multiple Redis
instances (and use database 0 only). This is because Redis is single-threaded
and using same instance will block when using any of its databases.
To configure multiple Redis instances:
https://www.digitalocean.com/community/questions/multiple-redis-instances-on-ubuntu-16-04

Also, consider Redis cluster
https://github.com/Grokzen/redis-py-cluster


UMLS Related Tools
------------------

py-umls: https://github.com/chb/py-umls
UMLS Description:
* http://text-analytics101.rxnlp.com/2013/11/what-tui-cui-lui-you-silly-sui.html
* https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2001-108.pdf
