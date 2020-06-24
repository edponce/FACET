MODULES
=======


FACET Command Line Interface
----------------------------

.. click:: facet:cli
    :prog: facet
    :show-nested:


Default FACET
-------------

.. autoclass:: facet.base.BaseFacet
    :members:

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

.. autoclass:: facet.RedisDatabase
    :members:

.. autoclass:: facet.DictDatabase
    :members:

.. autoclass:: facet.Elasticsearchx
    :members:

.. autoclass:: facet.ElasticsearchDatabase
    :members:


N-gram Features Extractor
-------------------------

.. autoclass:: facet.simstring.ngram.BaseNgram
    :members:

.. autoclass:: facet.CharacterNgram
    :members:

.. autoclass:: facet.WordNgram
    :members:


Similarity Measures
-------------------

.. autoclass:: facet.simstring.similarity.BaseSimilarity
    :members:

.. autoclass:: facet.ExactSimilarity

.. autoclass:: facet.DiceSimilarity

.. autoclass:: facet.CosineSimilarity

.. autoclass:: facet.JaccardSimilarity

.. autoclass:: facet.OverlapSimilarity

.. autoclass:: facet.HammingSimilarity


Database Serializer
-------------------

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

.. autoclass:: facet.Simstring
    :members:

.. autoclass:: facet.ElasticsearchSimstring
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

.. autoclass:: facet.SimpleFormatter
    :members:
