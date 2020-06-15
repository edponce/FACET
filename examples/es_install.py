import QuickerUMLS


##########
# CONFIG #
##########
umls = 'data/umls_midsmall'
index = 'umls_midsmall'


###########
# PROCESS #
###########
db = QuickerUMLS.RedisDatabase(db=2)
ss = QuickerUMLS.ESSimstring(db=index)

# Install
facet = QuickerUMLS.ESInstaller(cuisty_db=db, simstring=ss)
facet.install(umls)
