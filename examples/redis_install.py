import QuickerUMLS


##########
# CONFIG #
##########
umls = 'data/umls_large'


###########
# PROCESS #
###########
db1 = QuickerUMLS.RedisDatabase(db=0)
db2 = QuickerUMLS.RedisDatabase(db=1)
ss = QuickerUMLS.Simstring(db=QuickerUMLS.RedisDatabase(db=2))

# Install
ins = QuickerUMLS.Installer(conso_db=db1, cuisty_db=db2, simstring=ss)
ins.install(umls)
