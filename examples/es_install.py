import QuickerUMLS


##########
# CONFIG #
##########
umls = 'data/umls_large'


###########
# PROCESS #
###########
db = QuickerUMLS.RedisDatabase(db=2)
ss = QuickerUMLS.ESSimstring(db='testing')

# Install
facet = QuickerUMLS.ESInstaller(cuisty_db=db, simstring=ss)
facet.install(umls)
