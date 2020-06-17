MODULES
=======

Installer API
-------------

.. autoclass:: facet.Installer
    :members:

.. autoclass:: facet.ESInstaller
    :members:


FACET API
---------

.. autoclass:: facet.Facet
    :members:

.. autoclass:: facet.ESFacet
    :members:


Tokenizer API
-------------

.. autoclass:: facet.WhitespaceTokenizer
    :members:

.. autoclass:: facet.SpacyTokenizer
    :members:

.. autoclass:: facet.NLTKTokenizer
    :members:


Database API
------------

.. autoclass:: facet.database.base.BaseDatabase
    :members:

.. autoclass:: facet.RedisDatabase
    :members:

.. autoclass:: facet.DictDatabase
    :members:

.. autoclass:: facet.Elasticsearchx
    :members:

.. autoclass:: facet.ElasticsearchDatabase
    :members:


Ngram Features
--------------

.. autoclass:: facet.CharacterFeatures
    :members:

.. autoclass:: facet.WordFeatures
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

.. autoclass:: facet.serializer.base.BaseSerializer
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


Simstring
---------

.. autoclass:: facet.Simstring
    :members:

.. autoclass:: facet.ESSimstring
    :members:


Formatter
---------

.. autoclass:: facet.Formatter
    :members:
