import QuickerUMLS


##########
# CONFIG #
##########
text = 'data/test2.txt'
format = 'json'
# outfile = 'es_example.json'
outfile = None
# tokenizer = QuickerUMLS.NLTKTokenizer()
# tokenizer = QuickerUMLS.SpacyTokenizer()
tokenizer = QuickerUMLS.WhitespaceTokenizer()


###########
# PROCESS #
###########
db = QuickerUMLS.RedisDatabase(db=2)
ss = QuickerUMLS.ESSimstring(db='testing')

# Search
m = QuickerUMLS.ESFacet(cuisty_db=db, simstring=ss, tokenizer=tokenizer)
m.formatter.format = format
m.formatter.outfile = outfile
data = m(text)


##########
# OUTPUT #
##########
print(data)
