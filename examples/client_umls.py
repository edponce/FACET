import facet


factory = facet.FacetFactory('config/umls.yaml:UMLSES_SEARCH')

host = 'localhost'
port = 4444
with facet.network.SocketClient(
    (host, port),
    target_class=facet.facets.facet_map[factory._config['class']],
) as f:
    matches = f.match('acetate')
    print(matches)
