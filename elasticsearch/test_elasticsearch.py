# Install:
#   wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
#   /etc/apt/sources.list.d/elasticsearch.list
#       deb https://artifacts.elastic.co/packages/7.x/apt stable main
#   apt update
#   apt install elasticsearch
#   /etc/elasticsearch/elasticsearch.yml
#       node.name: node-1
#       node.master: true
#       node.data: true
#       network.host: 0.0.0.0
#       http.port: 9200
#       transport.host: localhost
#       transport.tcp.port: 9300
#       cluster.initial_master_nodes: ["node-1"]
#   pip install elasticsearch
# systemctl enable elasticsearch
# systemctl start elasticsearch
# systemctl stop elasticsearch
# Logs are stored in /var/log/elasticsearch
# Data is stored in
#   CentOS - /var/lib/elasticsearch
#   Ubuntu - /var/lib/elasticsearch/data

# Elasticsearch score is based on BM25 TF-IDF
# https://en.wikipedia.org/wiki/Okapi_BM25
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk

es = Elasticsearch('http://localhost:9200')
index = 'test'
terms = ['eduardo', 'eduard']
queries = ['edardo', 'onec']

# Insert/index
# print('Manual index')
# terms = ['eduardo', 'eduard']
# for term in terms:
#     res = es.index(index=index, body={'term': term})
#     print(res)

# https://elasticsearch-py.readthedocs.io/en/master/helpers.html
# https://github.com/elastic/elasticsearch-py/blob/master/example/load.py#L76-L130
# print('Streaming Bulk index')
# def gendata(data):
#     for datum in data:
#         yield {'_op_type': 'index', '_index': index, '_type': '_doc', '_source': {'term': datum}}
# terms = ['one', 'once']
# res = bulk(es, gendata(terms), chunk_size=500)
# print(res)

# print('Parallel Bulk index')
# def gendata(data):
#     for datum in data:
#         yield {'_op_type': 'index', '_index': index, '_type': '_doc', '_source': {'term': datum}}
# terms = ['one', 'once']
# NOTE: Stores items as {'_source': {'doc': {'term': datum}}}
# res = parallel_bulk(es, gendata(terms), chunk_size=500, thread_count=8, queue_size=8)
# print(res)

# print('Bulk delete')
# def deldata(data):
#     for datum in data:
#         yield {'_op_type': 'delete', '_index': index, '_id': datum}
# ids = ['CrLqhmwBdKbLhGDe9o19', 'CbLqhmwBdKbLhGDe9o19']
# res = bulk(es, deldata(ids))
# print(res)

# print('Refresh index')
# res = es.indices.refresh(index=index)
# print(res)

# print('Delete index')
# res = es.indices.delete(index=index)
# print(res)

# print('Match all')
# res = es.search(index=index, body={
#     'query': {
#         'match_all': {}
#     }
# })
# print(res)

print('Exact matching')
for query in queries:
    res = es.search(index=index, body={
        'query': {
            'match': {
                'term': query
            }
        }
    })
    print(res)

# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html
print('Fuzzy matching')
for query in queries:
    res = es.search(index=index, body={
        # Sort values based on score, 'asc' = ascending, 'desc' = descending
        'sort': [{'_score': 'desc'}],
        'query': {
            'fuzzy': {
                'term': {
                    'value': query,
                    # Maximum Levenshtein edit distance, the number of
                    # character changes needed to match strings
                    # https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#fuzziness
                    'fuzziness': 'AUTO',
                    # Number of initial characters to skip for fuzzy match
                    'prefix_length': 0,
                    # Maximum number of terms that fuzzy query will expand to
                    'max_expansions': 50,
                    # Enable/disable transpositions, ab -> ba
                    'transpositions': 'true'
                }
            }
        }
    })
    print(res)
