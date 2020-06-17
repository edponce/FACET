import facet


##########
# CONFIG #
##########
umls = 'data/umls_midsmall'


###########
# PROCESS #
###########
db1 = facet.RedisDatabase(db=0)
db2 = facet.RedisDatabase(db=1)
db3 = facet.DictDatabase('db/umls_midsmall')
ss = facet.Simstring(db=db3)

# Install
facet = facet.Installer(conso_db=db1, cuisty_db=db2, simstring=ss)
facet.install(umls)
