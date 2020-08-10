import facet


# Connect to databases
db1 = facet.SQLiteDatabase(
    uri='db/umls',
    table='conso',
    access_mode='c',
    use_pipeline=True,
    connect=True,
    serializer='pickle',
)
db2 = facet.SQLiteDatabase(
    uri='db/umls',
    table='cuisty',
    access_mode='c',
    use_pipeline=True,
    connect=True,
    serializer='pickle',
)
db3 = facet.SQLiteDatabase(
    uri='db/umls',
    table='simstring',
    access_mode='c',
    use_pipeline=True,
    connect=True,
    serializer='pickle',
)

# Create Simstring instance
ss = facet.Simstring(db=db3)

# Create FACET instance and install
f = facet.UMLSFacet(
    conso_db=db1,
    cuisty_db=db2,
    matcher=ss,
)

# This is the path (directory) where MRCONSO.RRF and MRSTY.RRF reside
f.install('data/umls')
