# coding: utf-8
# Simple benchmark script when searching similar strings by using elasticsearch instead of SimString.
# Since Elasticsearch uses Apache Lucene, TF/IDF based searching algorithm, the purpose for searching text will be different from this library.

import os, sys
sys.path.append(os.getcwd())
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from benchmarker import Benchmarker


SEARCH_COUNT_LIMIT = 10**4


def output_similar_strings_of_each_line(ipath, opath='tmp.strings'):
    db = Elasticsearch('http://localhost:9200')
    index = 'simstring'
    number_of_lines = len(open(ipath).readlines())

    with Benchmarker(width=20) as bench:
        @bench("initialize database({0} lines)".format(number_of_lines))
        def _(bm):
            def gendata(ipath):
                with open(ipath) as fd:
                    for line in fd:
                        string = line.rstrip('\r\n')
                        yield {'_index': index, '_source': {'strings': string}}
            res = bulk(db, gendata(ipath), chunk_size=500)

        @bench("search text({0} times)".format(min(number_of_lines, SEARCH_COUNT_LIMIT)))
        def _(bm):
            with open(ipath) as ifd, open(opath, 'w') as ofd:
                query_cmd = {
                        'sort': [{'_score': 'desc'}],
                        'query': {
                            'fuzzy': {
                                'strings': {
                                    'value': '',
                                    'fuzziness': 'AUTO',
                                    'prefix_length': 0,
                                    'max_expansions': 50,
                                    'transpositions': 'true'
                                }
                            }
                        }
                    }
                # query_cmd = {
                #         'query': {
                #             'match': {
                #                 'strings': ''
                #             }
                #         }
                #     }
                for i, line in enumerate(ifd):
                    if i >= SEARCH_COUNT_LIMIT:
                        break
                    string = line.rstrip('\r\n')
                    query_cmd['query']['fuzzy']['strings']['value'] = string
                    # query_cmd['query']['match']['strings'] = string
                    result = db.search(index=index, body=query_cmd)

                    if len(result['hits']['hits']) > 0:
                        results = []
                        for hit in result['hits']['hits']:
                            results.append(hit['_source']['strings'])
                        ofd.write(f"{string}: {', '.join(results)}\n")


data = './dev/data/company_names.txt'
print('benchmark for using Elasticsearch as database')
output_similar_strings_of_each_line(data, './dev/elasticsearch.strings')
