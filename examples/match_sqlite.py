import facet


# tokenizer = facet.SimpleTokenizer()
# tokenizer = facet.NLTKTokenizer()
# tokenizer = facet.SpacyTokenizer()
tokenizer = facet.WhitespaceTokenizer()


# Connect to databases
db1 = facet.SQLiteDatabase(
    uri='db/umls',
    table='conso',
    access_mode='r',
    use_pipeline=False,
    connect=True,
    serializer='pickle',
)
db2 = facet.SQLiteDatabase(
    uri='db/umls',
    table='cuisty',
    access_mode='r',
    use_pipeline=False,
    connect=True,
    serializer='pickle',
)
db3 = facet.SQLiteDatabase(
    uri='db/umls',
    table='simstring',
    access_mode='r',
    use_pipeline=False,
    connect=True,
    serializer='pickle',
)

# Create Simstring instance
ss = facet.Simstring(
    db=db3,
    alpha=0.7,
    similarity='cosine',
)

# Create FACET instance and match
f = facet.UMLSFacet(
    conso_db=db1,
    cuisty_db=db2,
    matcher=ss,
    tokenizer=tokenizer,
    formatter='json',
)

query = ['acetate', 'data/sample.txt']
matches = f.match(query, output=None)
print(matches)
