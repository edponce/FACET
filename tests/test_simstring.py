from QuickerUMLS import toolbox


word = 'cancer'
simstring_db = 'umls-2018-AA/umls-simstring.db'
ss_db = toolbox.SimstringDBReader(simstring_db, 'jaccard', 0.7)
print(ss_db.get(word))
