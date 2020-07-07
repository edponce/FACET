import facet


# tokenizer = None
# tokenizer = facet.SimpleTokenizer()
# tokenizer = facet.NLTKTokenizer()
# tokenizer = facet.SpacyTokenizer()
# tokenizer = facet.WhitespaceTokenizer()


db1 = facet.RedisDatabase(db=0)
db2 = facet.RedisDatabase(db=1)
db3 = facet.DictDatabase('db/umls_midsmall', flag='r')
# cdb = facet.RedisDatabase(db=2)
ss = facet.Simstring(
    db=db3,
    cache_db=None,
    alpha=0.7,
    similarity='cosine',
)

f = facet.UMLSFacet(
    conso_db=db1,
    cuisty_db=db2,
    matcher=ss,
    tokenizer='ws',
    formatter='json',
)

# query = 'data/katie_note.txt'
# query = 'data/synthetic_note.txt'
query = 'acetate'

data = f.match(query, output=None)
print(data)
