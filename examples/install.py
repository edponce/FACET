import QuickerUMLS


##########
# CONFIG #
##########
umls = 'data/umls_midsmall'


###########
# PROCESS #
###########
db1 = QuickerUMLS.RedisDatabase(db=0)
db2 = QuickerUMLS.RedisDatabase(db=1)
db3 = QuickerUMLS.DictDatabase('db/umls_midsmall')
ss = QuickerUMLS.Simstring(db=db3)

# Install
facet = QuickerUMLS.Installer(conso_db=db1, cuisty_db=db2, simstring=ss)
facet.install(umls)
