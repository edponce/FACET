import QuickerUMLS


##########
# CONFIG #
##########
text = 'data/test.txt'
umls = 'data/umls_medium'
format = 'json'
outfile = 'dict_example.json'


###########
# PROCESS #
###########
db1 = QuickerUMLS.DictDatabase()
db2 = QuickerUMLS.DictDatabase()
ss = QuickerUMLS.ESSimstring(db='testing')

# Install
facet = QuickerUMLS.Installer(conso_db=db1, cuisty_db=db2, simstring=ss)
facet.install(umls)

# Search
ss = QuickerUMLS.ESSimstring(
    db=QuickerUMLS.ElasticsearchDatabase(index='testing'),
)
m = QuickerUMLS.Facet(conso_db=db1, cuisty_db=db2, simstring=ss)
m.formatter.format = format
m.formatter.outfile = outfile
data = m(text)


##########
# OUTPUT #
##########
print(data)
