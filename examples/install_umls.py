import facet


factory = facet.FacetFactory('config/umls.yaml:UMLSES_INSTALL')

f = factory.create()

f.install('data/umls', nrows=100000)
