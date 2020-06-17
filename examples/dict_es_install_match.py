import QuickerUMLS


##########
# CONFIG #
##########
text = 'data/test_text.txt'
umls = 'data/umls_midsmall'
format = 'json'
outfile = 'dict_example.json'
index = 'umls_midsmall'


###########
# PROCESS #
###########
db1 = QuickerUMLS.DictDatabase()
db2 = QuickerUMLS.DictDatabase()
ss = QuickerUMLS.ESSimstring(db=index)

# Install
facet = QuickerUMLS.Installer(conso_db=db1, cuisty_db=db2, simstring=ss)
facet.install(umls)

# Search
ss = QuickerUMLS.ESSimstring(
    db=QuickerUMLS.ElasticsearchDatabase(index=index),
)
m = QuickerUMLS.Facet(conso_db=db1, cuisty_db=db2, simstring=ss)
m.formatter.format = format
m.formatter.outfile = outfile
data = m(text)


##########
# OUTPUT #
##########
print(data)
