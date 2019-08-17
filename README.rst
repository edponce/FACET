.. .. image:: https://travis-ci.org/kbrown42/quickerumls.svg?branch=master
   :target: https://travis-ci.org/kbrown42/quickerumls
   :alt: Tests Status

.. .. image:: https://codecov.io/gh/kbrown42/quickerumls/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/edponce/quickerumls
   :alt: Coverage Status

.. .. image:: https://readthedocs.org/projects/quickerumls/badge/?version=latest
   :target: https://quickerumls.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://img.shields.io/badge/license-MIT-blue.svg
   :target: https://github.com/edponce/smarttimers/blob/master/LICENSE
   :alt: License

|

QuickerUMLS
===========

QuickerUMLS is an extension of `QuickUMLS`_ tool providing faster, scalable,
and flexible concept extraction from medical narratives.

Before Starting and System Initialize
-------------------------------------

1. Python installation should include C headers (python3-dev).
2. Run all the setup scripts in 'setup' directory.
   * setup_simstring.sh
   * setup_spacy.sh
   * setup_umls.sh: download UMLS and initialize the system (see 'Initialize System')
3. Install package, pip install -e .
4. Check installation
   >>> python3 QuickerUMLS/install.py -h

Description of the NLM UMLS files is available at https://www.ncbi.nlm.nih.gov/books/NBK9685.


Database Initialization
-----------------------

1. Create Simstring and LevelDB UMLS database (
>>> python3 QuickerUMLS/install.py --lowercase --normalize-unicode --umls-dir /path/to/UMLS/RRF/files --install-dir /path/to/install/UMLS/database
>>> python3 QuickerUMLS/install.py -l -n -u /path/to/UMLS/RRF/files -i /path/to/database/installation

The following are results for UMLS 2018-AA (8,015,988 concepts).

Loading concepts: 2.05e-05 sec
Writing concepts: 247.99 sec
Loading semantic types: 6.43e-06 sec
Writing Simstring database: 468.66 sec
Writing semantic types: 9.87 sec


=====================  ===========  ====================
Task                   Runtime (s)  Comments
=====================  ===========  ====================
Load concepts          0.0000205    File (pandas)
Write concepts         247.99       Level DB
Write Simstring DB     444.90       Files (Simstring DB)
Load semantic types    0.02         File (pandas)
Write semantic types   10.10        Level DB
Total install          736.32
=====================  ===========  ====================


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


Benchmarks
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


QuickUMLS
=========

Before Starting
---------------

1. Python installation should include C headers (python3-dev).
2. You can install requirements manually, pip install -r requirements.
3. In order to use spaCy, download the relevant corpus, python3 -m spacy download en.
4. You require to have a valid UMLS installation on disk. To install UMLS, you
   must first obtain a `UMLS license`_ from the National Library of Medicine,
   then download all `UMLS files`_. Finally, you can install UMLS using the
   `MetamorphoSys`_ tool. The installation can be removed once the system has
   been initialized.

Initialize system
-----------------

1. Download and compile `Simstring`_, bash setup_simstring.sh 3.
2. Initialize the system by running, python install.py <umls_installation_path> <destination_path>. This process takes between between 30 minutes and forever.
   * <umls_installation_path> is the directory of the UMLS installation (in particular, we need MRCONSO.RRF and MRSTY.RRF).
   * <destination_path> is the directory where the QuickUMLS data files will be installed.

     - -L, --lowercase: Fold all concept terms to lowercase before being processed.
       This option typically increases recall, but it might reduce precision.
     - -U, --normalize-unicode: Expressions with non-ASCII characters are converted
       to the closest combination of ASCII characters.
     - -E, --language: Specify the language to consider for UMLS concepts (defuault
       is English). For a complete list of languages, see `NLM language table`_.


.. _QuickUMLS: https://github.com/Georgetown-IR-Lab/QuickUMLS
.. _UMLS license: https://uts.nlm.nih.gov/license.html
.. _UMLS files: https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html
.. _MetamorphoSys: https://www.nlm.nih.gov/research/umls/implementation_resources/metamorphosys/help.html
.. _Simstring: http://www.chokkan.org/software/simstring
.. _NLM language table: https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/abbreviations.html#LAT
.. _Hazardous or Poisonous Substance: https://metamap.nlm.nih.gov/Docs/SemanticTypes_2018AB.txt


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
        - plyvel._plyvel.IOError: b'IO error: lock test.db/LOCK: Resource temporarily unavailable'


Plyvel Info:
    * Uses Cython, can be installed manually on system (repo contains Dockerfile). This might be good to increase performance for the target architecture.


Plyvel API:
    * close() - closing the database while other threads are busy accessing it may result in hard crashes. Applications should make sure not to close databases that are concurrently used from other threads.
    * write_batch(transaction=False, sync=False) - create a WriteBatch instance for this database.
      - transaction - whether to enable transaction-like behaviour when used in 'with' block.
      - sync - whether to use synchronous writes
    * class WriteBatch - batch put/delete operations. Instances of this class can be used as context managers, when the 'with' block terminates, the batch will be automatically written to the database without an explicit call to 'WriteBatch.write()'.

    with db.write_batch() as b:
        b.put(b'key', b'value')


Pickle
======

Pickling objects may reduce their storage use when writing to a database or transferring data.

>>> import sys
>>> import pickle
>>> d = {'a': 1, 'b': 2}
>>> sys.getsizeof(d)  # 240 bytes
>>> sys.getsizeof(pickle.dumps(d))  # 61 bytes


spaCy
=====

>>> import spacy
>>> nlp = spacy.load('en')
>>> doc = nlp('very long text ...')
>>> ValueError: [E088] Text of length 1639120 exceeds maximum of 1000000. The v2.x parser and NER models require roughly 1GB of temporary memory per 100,000 characters in the input. This means long texts may cause memory allocation errors. If you're not using the parser or NER, it's probably safe to increase the `nlp.max_length` limit. The limit is in number of characters, so you can check whether your inputs are too long by checking `len(text)`.


Are Python lists thread-safe?
=============================

Lists themselves are thread-safe. In CPython the GIL protects against concurrent accesses to them, and other implementations take care to use a fine-grained lock or a synchronized datatype for their list implementations. However, while lists themselves can't go corrupt by attempts to concurrently access, the lists's data is not protected.


Python Multi-threading/processing
=================================

concurrent.futures.ThreadPoolExecutor
-------------------------------------

If max_workers is None or not given, it will default to the number of processors on the machine, multiplied by 5, assuming that ThreadPoolExecutor is often used to overlap I/O instead of CPU work


threading
---------

CPython implementation detail: In CPython, due to the Global Interpreter Lock, only one thread can execute Python code at once (even though certain performance-oriented libraries might overcome this limitation). If you want your application to make better use of the computational resources of multi-core machines, you are advised to use multiprocessing or concurrent.futures.ProcessPoolExecutor. However, threading is still an appropriate model if you want to run multiple I/O-bound tasks simultaneously.


Redis
=====

Redis database perform queries using a single-thread at a time (lock).

* Install Redis server/client packages in computer system (e.g., apt install redis-server).
* Install redis-py Python package (pip install redis).
