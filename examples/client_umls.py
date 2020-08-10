import facet


factory = facet.FacetFactory('config/umls_sqlite.yaml:Search')

host = 'localhost'
port = 4444
with facet.network.SocketClient(
    (host, port),
    target_class=factory.get_class(),
) as f:
    matches = f.match('acetate')
    print(matches)
