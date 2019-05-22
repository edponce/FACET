from QuickerUMLS import toolbox


# word = 'cancer'
word = 'memebers'
simstring_db = 'tmp/dict-simstring.db'
ss_db = toolbox.SimstringDBReader(simstring_db, 'jaccard', 0.7)
print(ss_db.get(word))
