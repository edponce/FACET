import facet


factory = facet.FacetFactory('config/umls.yaml:UMLSES_SEARCH')

f = factory.create()

matches = f.match('acetate')
print(matches)
