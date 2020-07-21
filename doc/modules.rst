MODULES
=======


FACET Command Line Interface
----------------------------

.. click:: facet.scripts.cli:cli
    :prog: facet
    :show-nested:


FACET API
---------

.. .. autoclass:: facet.facets.BaseFacet
..     :members:

.. autoclass:: facet.Facet
    :members:


UMLS FACET
----------

.. autoclass:: facet.UMLSFacet
    :members:


Tokenizer
---------

.. autoclass:: facet.tokenizer.BaseTokenizer
    :members:

.. autoclass:: facet.NullTokenizer
    :members:

.. autoclass:: facet.SymbolTokenizer
    :members:

.. autoclass:: facet.AlphaNumericTokenizer
    :members:

.. autoclass:: facet.WhitespaceTokenizer
    :members:

.. autoclass:: facet.SpaCyTokenizer
    :members:

.. autoclass:: facet.NLTKTokenizer
    :members:


Database
--------

.. autoclass:: facet.database.BaseDatabase
    :members:

.. autoclass:: facet.DictDatabase
    :members:

.. autoclass:: facet.RedisDatabase
    :members:

.. autoclass:: facet.RediSearchDatabase
    :members:

.. autoclass:: facet.SQLiteDatabase
    :members:

.. autoclass:: facet.ElasticsearchDatabase
    :members:

.. autoclass:: facet.MongoDatabase
    :members:


N-gram Features Extractor
-------------------------

.. autoclass:: facet.matcher.ngram.BaseNgram
    :members:

.. autoclass:: facet.CharacterNgram
    :members:

.. autoclass:: facet.WordNgram
    :members:


Similarity Measures
-------------------

.. autoclass:: facet.matcher.similarity.BaseSimilarity
    :members:

.. autoclass:: facet.ExactSimilarity

.. autoclass:: facet.DiceSimilarity

.. autoclass:: facet.CosineSimilarity

.. autoclass:: facet.JaccardSimilarity

.. autoclass:: facet.OverlapSimilarity

.. autoclass:: facet.HammingSimilarity


Serializer
----------

.. autoclass:: facet.serializer.BaseSerializer
    :members:

.. autoclass:: facet.JSONSerializer
    :members:

.. autoclass:: facet.YAMLSerializer
    :members:

.. autoclass:: facet.PickleSerializer
    :members:

.. autoclass:: facet.StringSerializer
    :members:

.. autoclass:: facet.StringSJSerializer
    :members:


Simstring - Approximate String Matching
---------------------------------------

.. autoclass:: facet.matcher.BaseSimstring
    :members:

.. autoclass:: facet.Simstring
    :members:

.. autoclass:: facet.ElasticsearchSimstring
    :members:


Fuzzy String Matching
---------------------

.. autoclass:: facet.ElasticsearchFuzzy
    :members:


Formatter
---------

.. autoclass:: facet.formatter.BaseFormatter
    :members:

.. autoclass:: facet.JSONFormatter
    :members:

.. autoclass:: facet.YAMLFormatter
    :members:

.. autoclass:: facet.XMLFormatter
    :members:

.. autoclass:: facet.PickleFormatter
    :members:

.. autoclass:: facet.CSVFormatter
    :members:

.. autoclass:: facet.NullFormatter
    :members:


Helpers
-------

.. autofunction:: facet.helpers.load_data

.. autofunction:: facet.helpers.iload_data

.. autofunction:: facet.helpers.corpus_generator
