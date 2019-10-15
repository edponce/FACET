from elasticsearch import Elasticsearch
from typing import Any, List, Dict, Tuple, Union, Iterable, Iterator
from elasticsearch.helpers import scan, bulk, parallel_bulk, streaming_bulk


__all__ = ['ElasticsearchDatabase']


class ElasticsearchDatabase(Elasticsearch):
    """Elasticsearch database interface extended with helper methods."""

    def xbulk(
        self,
        actions: Iterable[Dict[str, Any]],
        *,
        pipe: bool = False,
        thread_count: int = 1,
        **kwargs
    ) -> Tuple[int, List[Any]]:
        '''Extended bulk API.

        Args:
            actions (Iterable[Dict[str, Any]]): Actions for 'Helpers.*bulk()'.
                https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.bulk

            pipe (bool): If set, use streaming bulk instead of bulk API.
                Default is False.

            thread_count (int): Number of threads for parallel bulk API.
                If single threaded, use bulk API. Default is 1.

        Kwargs:
            Options forwarded to 'Helpers.*bulk()'.
        '''
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
        '''Bulk index and create.

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
            Options forwarded to 'xbulk()'.
        '''
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
        return self.xbulk(bodies, **kwargs)

    def bulk_delete(
        self,
        doc_ids: Iterable[str],
        *,
        index: str,
        doc_type: str = '_doc',
        **kwargs
    ) -> Tuple[int, List[Any]]:
        '''Bulk delete.

        Args:
            doc_ids (Iterable[str]): Document IDs to delete.

            index (str): Index name to search.

            doc_type (str): Document type. Default is '_doc'.

        Kwargs:
            Options forwarded to 'xbulk()'.
        '''
        bodies = ({
            '_op_type': 'delete',
            '_index': index,
            '_type': doc_type,
            '_id': doc_id,
        } for doc_id in doc_ids)
        return self.xbulk(bodies, **kwargs)

    def scan(
        self,
        body: Dict[str, Any],
        *,
        index: Union[str, List[str]],
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        '''
        Args:
            body (Dict[str, Any]): Body for 'Helpers.scan()'.
                https://elasticsearch-py.readthedocs.io/en/master/helpers.html#scan

            index (Union[str, List[str]]): Index names to search.
                https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search

        Kwargs:
            Options forwarded to 'Helpers.scan()'.
        '''
        return scan(self, body, index=index, **kwargs)
