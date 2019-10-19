import QuickerUMLS


##########
# CONFIG #
##########
text = 'data/test.txt'
format = 'json'
# outfile = 'es_example.json'
outfile = None
# tokenizer = QuickerUMLS.NLTKTokenizer()
# tokenizer = QuickerUMLS.SpacyTokenizer()
tokenizer = QuickerUMLS.WhitespaceTokenizer()


###########
# PROCESS #
###########
db1 = QuickerUMLS.RedisDatabase(db=2)
db2 = QuickerUMLS.RedisDatabase(db=3)
ss = QuickerUMLS.ESSimstring(
    db=QuickerUMLS.ElasticsearchDatabase(index='testing'),
)

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
