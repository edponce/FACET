import sys
import copy
import time
import collections
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


__all__ = ['ElasticsearchDatabase']


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

            thread_count (int): Number of threads for parallel bulk API.
                If single threaded, use bulk API.

        Kwargs: Options forwarded to 'elasticsearch.helpers.*bulk()' functions.

        Notes:
            * 'streaming_bulk()' and 'parallel_bulk()' return generators
              which need to be consumed for operation to complete.
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
                and 'create'.

            ids (Iterable[str]): Document ID. If set to None, ES sets
                random ones.

        Kwargs: Options forwarded to 'bulkx()'.
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

        Kwargs: Options forwarded to 'elasticsearch.helpers.scan()'.
        """
        return scan(self, body, index=index, **kwargs)


class ElasticsearchDatabase(BaseDatabase):
    """Elasticsearch database interface.

    Elasticsearch database interface with limited key/value related
    methods.

    Args:
        hosts (Any): Elasticsearch host(s).

        index (str): Index name.

        index_body (Dict[str, Any]): Mapping and settings for index.
            Only used during index creation.

        access_mode (str): Access mode for database.
            Valid values are: 'r' = read-only, 'w' = read/write,
            'c' = read/write/create if not exists, 'n' = new read/write.

        use_pipeline (bool): If set, queue 'set-related' commands to database.
            Run 'commit()' command to submit commands in pipe.

        stream (bool): If set, use streaming bulk instead of bulk API.

        thread_count (int): Number of threads for parallel bulk API.
            If single threaded, use bulk API.

        max_connect_attempts (int): Number of times to attempt connecting to
            database during object instantiation. There is no connection
            handling if connection disconnects at any other moment.

    Kwargs: Options forwarded to 'Elasticsearch()' via 'Elasticsearchx()'.
    """

    def __init__(
        self,
        index: str = 'facet',
        *,
        index_body: Dict[str, Any],
        hosts: Any = 'localhost:9200',
        access_mode: str = 'c',
        use_pipeline: bool = False,
        stream: bool = False,
        thread_count: int = 1,
        max_connect_attempts: int = 3,
        **conn_info,
    ):
        self._conn = None
        self._pipeline = None
        self._hosts = hosts
        self._index = index
        self._use_pipeline = use_pipeline
        self._stream = stream
        self._thread_count = thread_count
        self._max_connect_attempts = max_connect_attempts
        self._is_connected = False
        self._conn_info = copy.deepcopy(conn_info)

        self.connect()

        if access_mode == 'n':
            self.clear()

        if (
            access_mode in ('c', 'n')
            and not self._conn.indices.exists(index=index)
        ):
            self._conn.indices.create(
                index=index,
                body=index_body,
            )

    def __len__(self):
        return self._conn.count(index=self._index)['count']

    def __contains__(self, id):
        return NotImplemented

    def __getitem__(self, id):
        return NotImplemented

    def __setitem__(self, id, body):
        self.set(body, id=id)

    def __delitem__(self, id):
        self._conn.delete(index=self._index, id=id)

    def __iter__(self):
        return self.ids()

    def get_config(self, **kwargs):
        return {
            'hosts': self._hosts,
            'index': self._index,
            'memory usage': (
                self._conn.indices.stats(
                    index=self._index,
                    **kwargs,
                )['_all']['primaries']['store']['size_in_bytes']
                if self._is_connected
                else -1
            ),
            'item count': len(self) if self._is_connected else -1,
            'mapping': (
                self._conn.indices.get_mapping(index=self._index, **kwargs)
                if self._is_connected
                else {}
            ),
            'settings': (
                self._conn.indices.get_settings(index=self._index, **kwargs)
                if self._is_connected
                else {}
            ),
        }

    def get_info(self, **kwargs):
        return self._conn.info(**kwargs)

    def get_stats(self, **kwargs):
        return self._conn.indices.stats(index=self._index, **kwargs)

    def get(self, query: Dict[str, Any], *, key=None, **kwargs):
        """
        Args:
            key (Any): A hashable value that is unique for the document,
                that is used as a key for storing in pipeline dictionary.
        """
        if self._use_pipeline and key is not None and key in self._pipeline:
            return self._pipeline[key]
        return self._conn.search(index=self._index, body=query, **kwargs)

    def set(self, document: Dict[str, Any], *, key=None, **kwargs):
        """
        Args:
            key (Any): A hashable value that is unique for the document,
                that is used as a key for storing in pipeline dictionary.
        """
        if self._use_pipeline and key is not None:
            self._pipeline[key] = document
        else:
            self._conn.index(index=self._index, body=document, **kwargs)

    def scan(self, **kwargs):
        return self._conn.scan({}, index=self._index, **kwargs)

    def ids(self, **kwargs):
        return map(
            lambda x: x['_id'],
            self._conn.scan({}, index=self._index, **kwargs),
        )

    def items(self, **kwargs):
        return map(
            lambda x: (x['_id'], x['_source']),
            self._conn.scan({}, index=self._index, **kwargs),
        )

    def documents(self, **kwargs):
        return map(
            lambda x: x['_source'],
            self._conn.scan({}, index=self._index, **kwargs),
        )

    def delete(self, id, **kwargs):
        self._conn.delete(index=self._index, id=id, **kwargs)

    def connect(self):
        if self._is_connected:
            return
        connect_attempts = 0
        while True:
            connect_attempts += 1
            self._conn = Elasticsearchx(self._hosts, **self._conn_info)
            if self._conn.ping():
                break
            if connect_attempts >= self._max_connect_attempts:
                raise ConnectionError(
                    'failed to connect to Elasticsearch hosts'
                )
            print('Warning: failed connecting to Elasticsearch at '
                  f'{self._hosts}, reconnection attempt '
                  f'{connect_attempts} ...',
                  file=sys.stderr)
            time.sleep(1)
        if self._use_pipeline:
            self._pipeline = {}
        self._is_connected = True

    def commit(self, **kwargs):
        if self._is_connected and self._use_pipeline:
            if self._pipeline:
                # NOTE: deque consumes streaming/parallel bulk generators
                # and discards its results.
                collections.deque(
                    self._conn.bulk_index(
                        self._pipeline.values(),
                        index=self._index,
                        stream=kwargs.pop('stream', self._stream),
                        thread_count=kwargs.pop(
                            'thread_count',
                            self._thread_count,
                        ),
                        **kwargs,
                    ),
                    maxlen=0,
                )
                self._pipeline = {}
            # NOTE: Elasticsearch automatically triggers flushes as needed.
            # These flushes store data in transaction log to Lucene index.
            # self._conn.indices.flush(index=self._index)

    def disconnect(self):
        if self._is_connected:
            self._pipeline = None
            self._is_connected = False
            self._conn.close()

    def clear(self, **kwargs):
        if self._use_pipeline:
            self._pipeline = {}
        if self._conn.indices.exists(index=self._index):
            self._conn.indices.delete(index=self._index, **kwargs)
