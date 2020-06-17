import facet


##########
# CONFIG #
##########
# text = 'data/test_text.txt'
text = 'data/synthetic_note.txt'
# text = 'acetate'

format = 'json'

# outfile = 'redis_example.json'
outfile = None

tokenizer = facet.NLTKTokenizer()
# tokenizer = facet.SpacyTokenizer()
# tokenizer = facet.WhitespaceTokenizer()


###########
# PROCESS #
###########
db1 = facet.RedisDatabase(db=0)
db2 = facet.RedisDatabase(db=1)
db3 = facet.DictDatabase('db/umls_midsmall', flag='r')
ss = facet.Simstring(db=db3)
# cdb = facet.RedisDatabase(db=2)
# ss = facet.Simstring(db=db3, cache_db=cdb)

# Search
# m = facet.Facet(conso_db=db1, cuisty_db=db2, simstring=ss)
m = facet.Facet(
    conso_db=db1,
    cuisty_db=db2,
    simstring=ss,
    tokenizer=tokenizer)

data = m.match(text, format=format)
# data = m.match(text, alpha=0.7, format=format)
# data = m.match(text, alpha=0.7, format=format, outfile=outfile)


##########
# OUTPUT #
##########
print(data)
