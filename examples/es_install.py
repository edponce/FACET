import QuickerUMLS


##########
# CONFIG #
##########
umls = 'data/umls_small'


###########
# PROCESS #
###########
db1 = QuickerUMLS.RedisDatabase(db=2)
db2 = QuickerUMLS.RedisDatabase(db=3)
ss = QuickerUMLS.ESSimstring(db='testing')
# ss = QuickerUMLS.ESSimstring(db=QuickerUMLS.ElasticsearchDatabase(index='testing'))

# Install
facet = QuickerUMLS.Installer(conso_db=db1, cuisty_db=db2, simstring=ss)
facet.install(umls)
