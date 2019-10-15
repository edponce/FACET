Elastic Stack
=============

https://www.elastic.co/guide/en/kibana/current/index.html
https://www.digitalocean.com/community/tutorials/how-to-install-elasticsearch-logstash-and-kibana-elastic-stack-on-ubuntu-16-04

Elasticsearch - distributed RESTful search engine which stores data
Logstash - data processing component which sends incoming data to ES
Kibana - web interface for searching and visualizing logs
Beats - lightweight data shippers that can send data from hundreds/thousands of machines to either Logstash or ES


Requirements
------------

ufw
Java 8
Nginx


Install Elasticsearch
---------------------

wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
/etc/apt/sources.list.d/elasticsearch.list
    deb https://artifacts.elastic.co/packages/7.x/apt stable main
apt update
apt install elasticsearch
/etc/elasticsearch/elasticsearch.yml
    node.name: node-1
    node.master: true
    node.data: true
    network.host: 0.0.0.0
    http.port: 9200
    transport.host: localhost
    transport.tcp.port: 9300
    cluster.initial_master_nodes: ["node-1"]
pip install elasticsearch
systemctl enable elasticsearch
systemctl start elasticsearch
systemctl stop elasticsearch
Logs are stored in /var/log/elasticsearch
Data is stored in
  CentOS - /var/lib/elasticsearch
  Ubuntu - /var/lib/elasticsearch/data


Install Kibana
--------------

apt install kibana
systemctl enable kibana
systemctl start kibana
