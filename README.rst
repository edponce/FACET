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

Before Starting
---------------

1. Python installation should include C headers (python3-dev).
1. Install package, pip install -e .


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
