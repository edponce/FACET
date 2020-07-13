import facet


factory = facet.FacetFactory('config/umls.yaml:UMLSES_INSTALL')

f = factory.create()

# f.install('data/umls', nrows=100000)

host = 'localhost'
port = 4444
with facet.network.SocketServer(
    (host, port),
    facet.network.SocketServerHandler,
    served_object=f,
) as server:
    server.serve_forever()

f.close()
