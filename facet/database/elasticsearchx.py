from elasticsearch import Elasticsearch
from elasticsearch.helpers import (
    scan,
    bulk,
    parallel_bulk,
    streaming_bulk,
)
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Iterable,
    Iterator,
    Optional,
)


__all__ = ['ElasticsearchDatabase', 'Elasticsearchx']


class ElasticsearchDatabase:
    """Elasticsearch interface for Simstring algorithm.

    Args:
        hosts (Any): Elasticsearch hosts.

        index (str): Index name.

        fields (Iterable[str]): Document fields to use in search actions.
            Default is None (search disabled).

        body (Dict[str, Any]): Index settings and document mappings.

        pipe (bool): If set, queue 'set-related' commands to database.
            Run 'sync' command to submit commands in pipe.
            Default is False.

    Kwargs:
        Options forwarded to 'Elasticsearchx'.

    Interface mimics a key/value store:
        * set operations - keys are composites of int (ngram count)
                           and Iterable[str] (ngrams)
        * get operations - keys are composites of int (ngram count)
                           and str (joined ngrams)
        * get operations - keys are composites of Iterable[int]
                           (min/max ngram count) and str (joined ngrams)

    >>> db = ElasticsearchDatabase(index='default')
    >>> db.set({'term': 'hello',
    >>>         'ng': [' h' , 'he', 'el', 'll', 'lo', 'o '],
    >>>         'sz': 6})
    >>> db.get(6)
    >>> db.get(6, [' h', 'he', 'el', 'll', 'lo', 'o '])
    >>> db.get(6, [' h', 'he', 'el', 'll', 'lo', 'o '],
    >>>        filter_path=['hits.hits._source']) # _['hits']['hits]
    >>> db.get((5, 8), ['he', 'll'])  # range 5-7
    """

    def __init__(
        self,
        hosts: Any = None,
        *,
        index: str,
        fields: Iterable[str] = None,
        body: Dict[str, Any] = None,
        pipe: bool = False,
        overwrite: bool = False,
        **kwargs
    ):
        self._db = Elasticsearchx(hosts, **kwargs)
        self._index = index
        self.fields = fields
        if body is not None:
            if not self._db.indices.exists(index=self._index):
                self._db.indices.create(index=self._index, body=body)
            elif overwrite:
                self._db.indices.delete(index=self._index)
                self._db.indices.create(index=self._index, body=body)
        # NOTE: Stores an iterable of actions which
        # are sent to bulk API when sync() is invoked.
        self._dbp = None
        self._is_pipe = None
        self.set_pipe(pipe)

    def set_pipe(self, pipe):
        # NOTE: Invoke sync() when disabling pipe and pipe was enabled
        if not pipe:
            self.sync()
        self._is_pipe = pipe
        self._dbp = []

    def set(
        self,
        document: Dict[str, Any],
        *,
        doc_id: str = None,
        **kwargs
    ):
        """Index or create documents.

        Args:
            document (Dict[str, Any]): Document instance to index.

            doc_id (str): Document ID. If set to None, ES sets random ones.
                Default is None.

        Kwargs:
            Options forwarded to 'Elasticsearch.index()'.
        """
        if self._is_pipe:
            self._dbp.append(document)
        else:
            return self._db.index(
                index=self._index,
                body=document,
                id=doc_id,
                **kwargs
            )

    # NOTE: This can be removed, use pipe capability
    # def mset(self, documents, **kwargs):
    #     """See 'Elasticsearchx.bulk_index()'."""
    #     return self._db.bulk_index(documents, index=self._index, **kwargs)

    def sync(self, **kwargs):
        if self._is_pipe:
            response = self._db.bulk_index(
                self._dbp,
                index=self._index,
                **kwargs
            )
            self._dbp = []
            return response

    def get(
        self,
        key1: Union[int, Tuple[int, int]],
        key2: Iterable[str] = None,
        *,
        max_size: int = None,
        **kwargs
    ) -> Dict[str, Any]:
        body = self._resolve_search_body(key1, key2, max_size=max_size)
        return self._db.search(index=self._index, body=body, **kwargs)

    def scan(
        self,
        key1: Union[int, Tuple[int, int]],
        key2: Iterable[str] = None,
        *,
        max_size: int = None,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """See 'Elasticsearchx.scan()'."""
        body = self._resolve_search_body(key1, key2, max_size=max_size)
        return self._db.scan(body, index=self._index, **kwargs)

    def close(self):
        pass

    def _resolve_search_body(
        self,
        key1: Union[int, Tuple[int, int]],
        key2: Iterable[str] = None,
        *,
        max_size: int = None,
    ) -> Dict[str, Any]:
        if self.fields is None:
            raise KeyError('Search fields have not been set')

        if isinstance(key1, int):
            bool_body = {'must': {'match': {self.fields[0]: key1}}}
        else:
            bool_body = {'must': {'range': {
                self.fields[0]: {'gte': key1[0], 'lte': key1[1] - 1}
            }}}

        if key2 is not None:
            if not isinstance(key2, str):
                key2 = ' '.join(key2)
            bool_body.update({'filter': {'match': {self.fields[1]: key2}}})

        if max_size is not None and max_size > 0:
            body = {'size': max_size, 'query': {'bool': bool_body}}
        else:
            body = {'query': {'bool': bool_body}}
        return body


class Elasticsearchx(Elasticsearch):
    """Elasticsearch database interface extended with helper methods."""

    def bulkx(
        self,
        actions: Iterable[Dict[str, Any]],
        *,
        stream: bool = False,
        thread_count: int = 1,
        **kwargs
    ) -> Tuple[int, List[Any]]:
        """Extended bulk API.

        Args:
            actions (Iterable[Dict[str, Any]]): Actions for 'Helpers.*bulk()'.
                https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.bulk

            stream (bool): If set, use streaming bulk instead of bulk API.
                Default is False.

            thread_count (int): Number of threads for parallel bulk API.
                If single threaded, use bulk API. Default is 1.

        Kwargs:
            Options forwarded to 'Helpers.*bulk()'.
        """
        if stream:
            response = streaming_bulk(self, actions, **kwargs)
        else:
            if thread_count == 1:
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
        op_type: str = 'index',
        doc_id: Iterable[str] = None,
        **kwargs
    ) -> Tuple[int, List[Any]]:
        """Bulk index and create documents.

        Args:
            queries (Iterable[Dict[str, Any]]): List of document fields/values
                to index.

            index (str): Index name to search.

            op_type (str): Explicit operation type. Valid values are 'index'
                and 'create'. Default is 'index'.

            doc_id (Iterable[str]): Document ID. If set to None, ES sets
                random ones. Default is None.

        Kwargs:
            Options forwarded to 'bulkx()'.
        """
        if doc_id is None:
            bodies = ({
                '_op_type': op_type,
                '_index': index,
                '_source': query,
            } for query in queries)
        else:
            bodies = ({
                '_op_type': op_type,
                '_index': index,
                '_source': query,
                '_id': doc_id[i],
            } for i, query in enumerate(queries))
        return self.bulkx(bodies, **kwargs)

    def bulk_delete(
        self,
        doc_ids: Iterable[str],
        *,
        index: str,
        **kwargs
    ) -> Tuple[int, List[Any]]:
        """Bulk delete.

        Args:
            doc_ids (Iterable[str]): Document IDs to delete.

            index (str): Index name to search.

        Kwargs:
            Options forwarded to 'bulkx()'.
        """
        bodies = ({
            '_op_type': 'delete',
            '_index': index,
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
