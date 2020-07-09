import facet


# tokenizer = None
# tokenizer = facet.SimpleTokenizer()
# tokenizer = facet.NLTKTokenizer()
# tokenizer = facet.SpacyTokenizer()
tokenizer = facet.WhitespaceTokenizer()


# Connect to database
db1 = facet.DictDatabase('db/conso')
db2 = facet.DictDatabase('db/cuisty')
db3 = facet.DictDatabase('db/simstring')

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
