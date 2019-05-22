from QuickerUMLS import toolbox


# word = 'cancer'
# word = 'memebers'
# word = 'application'
word = 'availablity'
simstring_db = 'mimic-db/dict-simstring.db'
ss_db = toolbox.SimstringDBReader(simstring_db, 'jaccard', 0.8)
print(ss_db.get(word))
