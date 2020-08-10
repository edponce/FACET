import facet


def generate_sources(n):
    source = 'acetate'
    return [source] * n


factory = facet.FacetFactory('config/umls_redis.yaml:Search')

host = 'localhost'
port = 4444
with facet.network.SocketClient(
    (host, port),
    target_class=factory.get_class(),
) as f:
    matches = f.match(generate_sources(8))
    print(matches)
