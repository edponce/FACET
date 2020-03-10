import plyvel as leveldb
import pickle
import time


db = leveldb.DB('test.db', create_if_missing=True)
# db = leveldb.DB('test.db', create_if_missing=True, bloom_filter_bits=100)


# 10M keys
# 11.4 seconds (defaults)
# 11.9 seconds (transaction=True)
# 14.2 seconds (transaction=True, sync=True)
def not_sorted(dct):
    t0 = time.time()
    # with db.write_batch() as wb:
    with db.write_batch(transaction=True) as wb:
    # with db.write_batch(transaction=True, sync=True) as wb:
        for k in dct:
            wb.put(k.encode(), pickle.dumps(dct[k]))
    t1 = time.time()
    print('Write Time: ', t1 - t0)


# 10M keys
# 14.3 seconds
# 17.1 seconds (transaction=True)
# 16.5 seconds (transaction=True, sync=True)
def yes_sorted(dct):
    t0 = time.time()
    # with db.write_batch() as wb:
    with db.write_batch(transaction=True) as wb:
    # with db.write_batch(transaction=True, sync=True) as wb:
        for k in sorted(dct):
            wb.put(k.encode(), pickle.dumps(dct[k]))
    t1 = time.time()
    print('Write Time: ', t1 - t0)


# 10M keys
# 14.0 seconds
# 16.7 seconds (transaction=True)
# 16.2 seconds (transaction=True, sync=True)
def yes_sorted2(dct):
    keys = sorted(dct)
    t0 = time.time()
    # with db.write_batch() as wb:
    with db.write_batch(transaction=True) as wb:
    # with db.write_batch(transaction=True, sync=True) as wb:
        for k in keys:
            wb.put(k.encode(), pickle.dumps(dct[k]))
    t1 = time.time()
    print('Write Time: ', t1 - t0)


# 10M keys
# 8.9 seconds (defaults)
# 8.9 seconds (bloom_filter_bits=100)
def query(keys):
    t0 = time.time()
    for k in keys:
        v = db.get(k.encode())
    t1 = time.time()
    print('Read Time: ', t1 - t0)


# Create data
num_keys = 10000000
data = {}
for i in range(num_keys):
    data[str(num_keys - i)] = i


not_sorted(data)
# not_sorted(data)
# not_sorted(data)

# yes_sorted(data)
# yes_sorted(data)
# yes_sorted(data)

# yes_sorted2(data)
# yes_sorted2(data)
# yes_sorted2(data)


query(data.keys())
query(data.keys())
query(data.keys())
