from elasticsearch import Elasticsearch
from typing import Any, List, Dict, Tuple, Union, Iterable, Iterator
from elasticsearch.helpers import scan, bulk, parallel_bulk, streaming_bulk


__all__ = ['ElasticsearchDatabase', 'ElasticsearchX']


class ElasticsearchDatabase:
    """Elasticsearch interface for Simstring algorithm.

    Interface mimics a key-value store:
        * set operations - keys are composites of int (ngram count)
                           and Iterable[str] (ngrams)
        * get operations - keys are composites of int (ngram count)
                           and str (joined ngrams)
        * mget operations - keys are composites of Iterable[int]
                            (min/max ngram count)
                            and str (joined ngrams)
        * value is a str (value)

    es.set((6, [' h' , 'he', 'el', 'll', 'lo', 'o ']), 'hello')
    es.get((6, ' h he el ll lo o '))
    es.get((6, ' h he el ll lo o '),
           filter_path=['hits.hits._source'])['hits']['hits]
    es.get((4, 5, ' h he el ll lo o '))  # range 4-5
    """
    def __init__(
        self,
        hosts: Iterable[Dict[str, Any]] = None,
        *,
        index: str,
        pipe=False,
        **kwargs
    ):
        self._db = ElasticsearchX(hosts, **kwargs)
        self._index = index
        self._is_pipe = pipe

    # from elasticsearch_dsl import Search, Mapping, Document, Index
    # def create_index(self):
    #     mapping = Mapping()
    #     mapping.field('word', 'text', index=False)
    #     mapping.field('ngrams', 'text', norms=False, similarity='boolean')
    #     mapping.field('num_ngrams', 'integer', similarity='boolean')
    #
    #     index = Index(self._index, using=self)
    #     index.settings(
    #         number_of_shards=1,
    #         number_of_replicas=0,
    #     )
    #     index.mapping(mapping)
    #     index.create()

    def _get(
        self,
        key: Union[Tuple[int, str], Tuple[int, int, str]],
        *,
        # NOTE: Fields should not be hard-coded but mandatory
        fields: Iterable[str] = ('num_ngrams', 'ngrams'),
        **kwargs
    ) -> Dict[str, Any]:
        if len(key) == 2:
            must = {'match': {fields[0]: key[0]}}
        elif len(key) == 3:
            must = {'range': {fields[0]: {'gte': key[0], 'lte': key[1]}}}

        body = {'query': {'bool': {
            'must': must,
            'filter': {'match': {fields[-1]: key[-1]}}
        }}}

        # NOTE: Exclude values of explicit fields because client code
        # already passed those values.
        kwargs['_source_excludes'] = [
            *kwargs.get('_source_excludes', []),
            *fields,
        ]
        return self._db.search(index=self._index, body=body, **kwargs)

    def _set(
        self,
        body: Dict[str, Any],
        *,
        id_field: str = None,
        max_id_size: int = 20,
        **kwargs
    ):
        """Index or create documents.

        Args:
            query (Dict[str, Any]): Document field/values to index.

            id_field (str): Field from queries to use as document ID.
                If set to None, ES sets random ones. Default is None.

            max_id_size (int): Maximum number of characters for a custom
                document ID. Default is 20.

        Kwargs:
            Options forwarded to 'Elasticsearch.index()'.
        """
        return self._db.index(
            index=self._index,
            body=body,
            id=('' if id_field is None
                else body.get(id_field, '')[:max_id_size]),
            **kwargs
        )


class ElasticsearchX(Elasticsearch):
    """Elasticsearch database interface extended with helper methods."""

    def bulkx(
        self,
        actions: Iterable[Dict[str, Any]],
        *,
        pipe: bool = False,
        thread_count: int = 1,
        **kwargs
    ) -> Tuple[int, List[Any]]:
        """Extended bulk API.

        Args:
            actions (Iterable[Dict[str, Any]]): Actions for 'Helpers.*bulk()'.
                https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.bulk

            pipe (bool): If set, use streaming bulk instead of bulk API.
                Default is False.

            thread_count (int): Number of threads for parallel bulk API.
                If single threaded, use bulk API. Default is 1.

        Kwargs:
            Options forwarded to 'Helpers.*bulk()'.
        """
        if pipe:
            response = streaming_bulk(self, actions, **kwargs)
        else:
            if thread_count < 2:
                response = bulk(self, actions, **kwargs)
            else:
                response = parallel_bulk(
                    self,
                    actions,
                    thread_count=thread_count,
                    **kwargs
                )
        return response

    def bulk_index(
        self,
        queries: Iterable[Dict[str, Any]],
        *,
        index: str,
        doc_type: str = '_doc',
        op_type: str = 'index',
        id_field: str = None,
        max_id_size: int = 20,
        **kwargs
    ) -> Tuple[int, List[Any]]:
        """Bulk index and create documents.

        Args:
            queries (Iterable[Dict[str, Any]]): List of document field/values
                to index.

            index (str): Index name to search.

            doc_type (str): Document type. Default is '_doc'.

            op_type (str): Explicit operation type. Valid values are 'index'
                and 'create'. Default is 'index'.

            id_field (str): Field from queries to use as document ID.
                If set to None, ES sets random ones. Default is None.

            max_id_size (int): Maximum number of characters for a custom
                document ID. Default is 20.

        Kwargs:
            Options forwarded to 'bulkx()'.
        """
        if id_field is None:
            bodies = ({
                '_op_type': op_type,
                '_index': index,
                '_type': doc_type,
                '_source': query,
            } for query in queries)
        else:
            bodies = ({
                '_op_type': op_type,
                '_index': index,
                '_type': doc_type,
                '_source': query,
                '_id': query.get(id_field, '')[:max_id_size],
            } for query in queries)
        return self.bulkx(bodies, **kwargs)

    def bulk_delete(
        self,
        doc_ids: Iterable[str],
        *,
        index: str,
        doc_type: str = '_doc',
        **kwargs
    ) -> Tuple[int, List[Any]]:
        """Bulk delete.

        Args:
            doc_ids (Iterable[str]): Document IDs to delete.

            index (str): Index name to search.

            doc_type (str): Document type. Default is '_doc'.

        Kwargs:
            Options forwarded to 'bulkx()'.
        """
        bodies = ({
            '_op_type': 'delete',
            '_index': index,
            '_type': doc_type,
            '_id': doc_id,
        } for doc_id in doc_ids)
        return self.bulkx(bodies, **kwargs)

    def scan(
        self,
        body: Dict[str, Any],
        *,
        index: Union[str, List[str]],
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """Scan/scroll through documents.

        Args:
            body (Dict[str, Any]): Body for 'Helpers.scan()'.
                https://elasticsearch-py.readthedocs.io/en/master/helpers.html#scan

            index (Union[str, List[str]]): Index names to search.
                https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search

        Kwargs:
            Options forwarded to 'Helpers.scan()'.
        """
        return scan(self, body, index=index, **kwargs)
