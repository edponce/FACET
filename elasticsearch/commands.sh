#! /bin/sh

# https://www.bmc.com/blogs/elasticsearch-commands/

# Check ES service
curl -XGET http://localhost:9200

# List all indices
curl -XGET http://localhost:9200/_aliases?pretty=true

# List info of cluster and indices
curl -XGET http://localhost:9200/_cluster/health?pretty=true\&level=indices
curl -XGET http://localhost:9200/_cat/indices?v
curl -XGET http://localhost:9200/_stats?pretty=true

# Delete index
curl -XDELETE http://localhost:9200/simstring
curl -XDELETE http://localhost:9200/_all
