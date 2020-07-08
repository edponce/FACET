import facet


def test_facet():
    config = {'formatter': 'csv'}
    f = facet.FacetFactory(config).create()
    f.install('data/install/american-english', nrows=50000)
    matches = f.match('beautiful window in Apollo spacecraft')
    print(matches)
    f.close()
