from QuickerUMLS import toolbox


ss_db = toolbox.SimstringDBReader('umls-2018-AA/umls-simstring.db', 'jaccard', 0.7)
print(ss_db.get('cancer'))
