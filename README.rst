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
1. Run all the setup scripts in 'setup' directory.
   * setup_simstring.sh
   * setup_spacy.sh
   * setup_umls.sh: download UMLS and initialize the system
     (see 'Initialize System')
1. Install package, pip install -e .

Description of the NLM UMLS files is available at https://www.ncbi.nlm.nih.gov/books/NBK9685.


API and Usage
-------------

QuickUMLS(quickumls_fp, overlapping_criteria, threshold, similarity_name, window,
          accepted_semtypes)
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
    >>> matcher = QuickUMLS('path/to/UMLS/installation')
NOTE: this command will invoke NLTK which in turn downloads a package of stopwords
which are placed in the home directory. For English language there 179 stopwords.

Use the QuickUMLS object:
    >>> text = "The ulna has dislocated posteriorly from the trochlea of the humerus."
    >>> matches = matcher.match(text, best_match=True, ignore_syntax=False)
    >>> matches
    >>> [[{'start': 61, 'end':68, 'ngram': 'humerus', 'term': 'humerus', 'cui': 'C0020164', 'similarity': 1.0, 'semtypes': {'T023'}, 'preferred': 1}], [...]]

Set 'best_match' to 'False' if you want to return overlapping candidates.
Set 'ignore_syntax' to 'True' to disable all heuristics introduced in Soldaini
and Goharian 2016.


QuickUMLS
=========

Before Starting
---------------

1. Python installation should include C headers (python3-dev).
1. You can install requirements manually, pip install -r requirements.
1. In order to use spaCy, download the relevant corpus, python3 -m spacy download en.
1. You require to have a valid UMLS installation on disk. To install UMLS, you
   must first obtain a `UMLS license`_ from the National Library of Medicine,
   then download all `UMLS files`_. Finally, you can install UMLS using the
   `MetamorphoSys`_ tool. The installation can be removed once the system has
   been initialized.

Initialize system
-----------------

1. Download and compile `Simstring`_, bash setup_simstring.sh 3.
1. Initialize the system by running, python install.py <umls_installation_path>
   <destination_path>.
   <umls_installation_path> is the directory of the UMLS installation (in particular,
   we need MRCONSO.RRF and MRSTY.RRF).
   <destination_path> is the directory where the QuickUMLS data files will be
   installed.
   This process takes between between 30 minutes and forever.

   * -L, --lowercase: Fold all concept terms to lowercase before being processed.
     This option typically increases recall, but it might reduce precision.
   * -U, --normalize-unicode: Expressions with non-ASCII characters are converted
     to the closest combination of ASCII characters.
   * -E, --language: Specify the language to consider for UMLS concepts (defuault
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
