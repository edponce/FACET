import redis
import pickle

# Redis documentation:
# https://pypi.org/project/redis
# https://redis.io/commands

# Redis-Py requires a running Redis server:
# >> apt install redis-server
# >> redis-cli
# /etc/redis/redis.conf
# /var/lib/redis
# /etc/init.d/redis-server

# Redis client, 'pip install redis'

# Simple example
db = redis.Redis(host='localhost', port=6379, db=0)
print(db.get('foo'))
print(db.set('foo',  'bar'))
print(db.get('foo'))
print(db.set('hello', 'world'))
# print(db.mset({'foo': 'bar', 'hello': 'world'}))

# Read/write multiple items at a time
print(db.mget(['foo', 'hello']))

# Delete all items in current database
# print(db.flushdb())

# Delete all items in all database on the current host
# print(db.flushall())

# Store values consisting of a list
# {'foo': ['bar',1, 10.3], 'hello': 'world'}
b = pickle.dumps(['bar', 1, 10.3])
print(db.mset({'foo': b, 'hello': 'world'}))
print(db.get('foo'))
print(pickle.loads(db.get('foo')))

# If a key(s) exists, do not overwrite
# For multi-case, keys are updated only if all keys do not exist.
db.setnx('foo', 'rab')
db.msetnx({'foo': 'rab', 'hello': 'earth'})

# Connection pools (TCP)
# pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
# db = redis.Redis(connection_pool=pool)

# Connection pools (Unix domain socket)
# For cases where clients are running on the same device as the server.
# Requires enabling the 'unixsocket' parameter in 'redis.conf' file.
# db = redis.Redis(unix_socket_path='/tmp/redis.sock')
