import facet


db1 = facet.RedisDatabase(db=0)
db2 = facet.RedisDatabase(db=1)
db3 = facet.DictDatabase('db/umls_midsmall')
ss = facet.Simstring(db=db3)

f = facet.UMLSFacet(
    conso_db=db1,
    cuisty_db=db2,
    simstring=ss,
)
f.install('data/umls_midsmall')
