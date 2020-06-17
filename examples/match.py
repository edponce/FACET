import QuickerUMLS


##########
# CONFIG #
##########
# text = 'data/test_text.txt'
text = 'data/synthetic_note.txt'
# text = 'acetate'

format = 'json'

# outfile = 'redis_example.json'
outfile = None

tokenizer = QuickerUMLS.NLTKTokenizer()
# tokenizer = QuickerUMLS.SpacyTokenizer()
# tokenizer = QuickerUMLS.WhitespaceTokenizer()


###########
# PROCESS #
###########
db1 = QuickerUMLS.RedisDatabase(db=0)
db2 = QuickerUMLS.RedisDatabase(db=1)
db3 = QuickerUMLS.DictDatabase('db/umls_midsmall', flag='r')
ss = QuickerUMLS.Simstring(db=db3)
# cdb = QuickerUMLS.RedisDatabase(db=2)
# ss = QuickerUMLS.Simstring(db=db3, cache_db=cdb)

# Search
# m = QuickerUMLS.Facet(conso_db=db1, cuisty_db=db2, simstring=ss)
m = QuickerUMLS.Facet(
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
