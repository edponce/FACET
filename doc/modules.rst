MODULES
=======


FACET Command Line Interface
----------------------------

.. click:: facet:cli
    :prog: facet
    :show-nested:


Installer
---------

.. autoclass:: facet.Installer
    :members:

.. autoclass:: facet.ESInstaller
    :members:


Matcher
-------

.. autoclass:: facet.Facet
    :members:

.. autoclass:: facet.ESFacet
    :members:


Tokenizer
---------

.. autoclass:: facet.tokenizer.BaseTokenizer
    :members:

.. autoclass:: facet.WhitespaceTokenizer
    :members:

.. autoclass:: facet.SpacyTokenizer
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


Serializer
----------

.. autoclass:: facet.serializer.BaseSerializer
    :members:

.. autoclass:: facet.PickleSerializer
    :members:

.. autoclass:: facet.JSONSerializer
    :members:

.. autoclass:: facet.YAMLSerializer
    :members:

.. autoclass:: facet.StringSerializer
    :members:

.. autoclass:: facet.StringSJSerializer
    :members:


String Matching
---------------

.. autoclass:: facet.Simstring
    :members:

.. autoclass:: facet.ESSimstring
    :members:


Formatter
---------

.. autoclass:: facet.Formatter
    :members: