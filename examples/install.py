import facet


# Connect to database
db1 = facet.RedisDatabase(n=0)
db2 = facet.RedisDatabase(n=1)
db3 = facet.RedisDatabase(n=2)

# Create Simstring instance
ss = facet.Simstring(db=db3)

# Create FACET instance and install
f = facet.UMLSFacet(
    conso_db=db1,
    cuisty_db=db2,
    matcher=ss,
)

f.install('data/umls')
