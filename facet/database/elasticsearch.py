import copy
from elasticsearch import Elasticsearch
from elasticsearch.helpers import (
    scan,
    bulk,
    parallel_bulk,
    streaming_bulk,
)
from .base import BaseDatabase
from typing import (
    Any,
    List,
    Dict,
    Tuple,
    Union,
    Iterable,
    Iterator,
)


__all__ = [
    'ElasticsearchDatabase',
    'ElasticsearchKVDatabase',
]


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
        documents: Iterable[Dict[str, Any]],
        *,
        index: str,
        op_type: str = 'index',
        ids: Iterable[str] = None,
        **kwargs
    ) -> Tuple[int, List[Any]]:
        """Bulk index and create documents.

        Args:
            documents (Iterable[Dict[str, Any]]): List of document
                fields/values to index.

            index (str): Index name to search.

            op_type (str): Explicit operation type. Valid values are 'index'
                and 'create'. Default is 'index'.

            ids (Iterable[str]): Document ID. If set to None, ES sets
                random ones. Default is None.

        Kwargs:
            Options forwarded to 'bulkx()'.
        """
        if ids is None:
            actions = (
                {'_op_type': op_type, '_index': index, '_source': document}
                for document in documents
            )
        else:
            actions = (
                {
                    '_op_type': op_type,
                    '_index': index,
                    '_source': document,
                    '_id': id,
                } for id, document in zip(ids, documents)
            )
        return self.bulkx(actions, **kwargs)

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


class ElasticsearchDatabase(BaseDatabase):
    """Elasticsearch database interface.

    Args:
        host (Any): Elasticsearch host(s).

        index (str): Index name.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit()' command to submit commands in pipe.
            Default is False.

    Kwargs:
        Options forwarded to 'Elasticsearch()' via 'Elasticsearchx()'.
    """

    def __init__(
        self,
        host: Any = 'localhost',
        *,
        index: str = 'facet',
        access_mode: str = 'c',
        use_pipeline: bool = False,
        **conn_info,
    ):
        self._db = None
        self._dbp = []
        self._host = host
        self._index = index
        self._use_pipeline = use_pipeline
        self._is_connected = False
        self._conn_info = copy.deepcopy(conn_info)

        self.connect()

        # Reset database based on access mode
        if access_mode == 'n':
            self.clear()

        if (
            access_mode in ('c', 'n')
            and not self._db.indices.exists(index=index)
        ):
            self._db.indices.create(
                index=index,
                body=self._conn_info.pop('body', None),
            )

    def __len__(self):
        return self._db.count(index=self._index)['count']

    def __contains__(self, id):
        return NotImplemented

    def get_config(self, **kwargs):
        return {
            'host': self._host,
            'index': self._index,
            'memory usage': (
                self._db.indices.stats(
                    index=self._index,
                    **kwargs,
                )['_all']['primaries']['store']['size_in_bytes']
                if self._is_connected
                else -1
            ),
            'item count': len(self) if self._is_connected else -1,
            'mapping': (
                self._db.indices.get_mapping(index=self._index, **kwargs)
                if self._is_connected
                else {}
            ),
            'settings': (
                self._db.indices.get_settings(index=self._index, **kwargs)
                if self._is_connected
                else {}
            ),
        }

    def get_info(self, **kwargs):
        return self._db.info(**kwargs)

    def get_stats(self, **kwargs):
        return self._db.indices.stats(index=self._index, **kwargs)

    def get(self, query: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return self._db.search(index=self._index, body=query, **kwargs)

    def set(self, document: Dict[str, Any], **kwargs):
        if self._use_pipeline:
            self._dbp.append(document)
        else:
            self._db.index(index=self._index, body=document, **kwargs)

    def scan(self, **kwargs):
        return self._db.scan({}, index=self._index, **kwargs)

    def delete(self, id, **kwargs):
        self._db.delete(index=self._index, id=id, **kwargs)

    def connect(self):
        self._db = Elasticsearchx(self._host, **self._conn_info)
        self._is_connected = True

    def commit(self, **kwargs):
        if self._is_connected and self._use_pipeline:
            response = self._db.bulk_index(
                self._dbp,
                index=self._index,
                **kwargs,
            )
            self._dbp = []
            # self._db.flush(index=self._index, **kwargs)
            return response

    def disconnect(self):
        if self._is_connected:
            self._db.close()
            self._dbp = []
            self._is_connected = False

    def clear(self, **kwargs):
        self._db.indices.delete(index=self._index, **kwargs)
        self._dbp = []


class ElasticsearchKVDatabase(ElasticsearchDatabase):
    """Elasticsearch database interface with limited key/value related
    methods.

    Notes:
        * In special methods, key = document ID, and value = document.
    """
    def __getitem__(self, id):
        return NotImplemented

    def __setitem__(self, id, body):
        self.set(body, id=id)

    def __delitem__(self, id):
        self._db.delete(index=self._index, id=id)

    def __iter__(self):
        return self.ids()

    def ids(self, **kwargs):
        return map(
            lambda x: x['_id'],
            self._db.scan({}, index=self._index, **kwargs),
        )

    def items(self, **kwargs):
        return map(
            lambda x: (x['_id'], x['_source']),
            self._db.scan({}, index=self._index, **kwargs),
        )

    def documents(self, **kwargs):
        return map(
            lambda x: x['_source'],
            self._db.scan({}, index=self._index, **kwargs),
        )
