import facet


# Connect to database
db1 = facet.DictDatabase('db/conso', use_pipeline=True)
db2 = facet.DictDatabase('db/cuisty', use_pipeline=True)
db3 = facet.DictDatabase('db/simstring', use_pipeline=True)

# Create Simstring instance
ss = facet.Simstring(db=db3)

# Create FACET instance and install
f = facet.UMLSFacet(
    conso_db=db1,
    cuisty_db=db2,
    matcher=ss,
    use_proxy_install=True,
)

# This is the path (directory) where MRCONSO.RRF and MRSTY.RRF reside
f.install('data/umls')
