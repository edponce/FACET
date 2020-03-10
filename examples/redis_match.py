import QuickerUMLS


##########
# CONFIG #
##########
text = 'data/test.txt'
format = 'json'
# outfile = 'redis_example.json'
outfile = None
# tokenizer = QuickerUMLS.NLTKTokenizer()
# tokenizer = QuickerUMLS.SpacyTokenizer()
tokenizer = QuickerUMLS.WhitespaceTokenizer()


###########
# PROCESS #
###########
db1 = QuickerUMLS.RedisDatabase(db=0)
db2 = QuickerUMLS.RedisDatabase(db=1)
ss = QuickerUMLS.Simstring(db=QuickerUMLS.RedisDatabase(db=2))

# Search
m = QuickerUMLS.Facet(
    conso_db=db1,
    cuisty_db=db2,
    simstring=ss,
    tokenizer=tokenizer)
m.formatter.format = format
m.formatter.outfile = outfile
data = m(text)


##########
# OUTPUT #
##########
print(data)
