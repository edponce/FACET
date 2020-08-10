import facet


# Install
factory = facet.FacetFactory('config/umls_redis.yaml:Install')

tmp_f = factory.create()
tmp_f.install('data/umls', nrows=40000)

# Parallel matcher
f = facet.ParallelFacet('config/umls_redis.yaml:Search')

host = 'localhost'
port = 4444
with facet.network.SocketServer(
    (host, port),
    facet.network.SocketServerHandler,
    served_object=f,
) as server:
    server.serve_forever()

f.close()
