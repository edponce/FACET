import facet


# tokenizer = None
# tokenizer = facet.SimpleTokenizer()
# tokenizer = facet.NLTKTokenizer()
# tokenizer = facet.SpacyTokenizer()
tokenizer = facet.WhitespaceTokenizer()


# Connect to database
db1 = facet.RedisDatabase(n=0)
db2 = facet.RedisDatabase(n=1)
db3 = facet.RedisDatabase(n=2)
# cdb = facet.RedisDatabase(n=3)
cdb = None

# Create Simstring instance
ss = facet.Simstring(
    db=db3,
    cache_db=cdb,
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

query = ['acetate', 'cancer', 'ulna', 'data/sample.txt']
matches = f.match(query, output=None)
print(matches)
