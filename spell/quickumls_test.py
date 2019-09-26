from quickumls import QuickUMLS

quickumls_fp = '/mnt/d/projects/quickumls'
matcher = QuickUMLS(quickumls_fp)

text = "The ulna has dislocated posteriorly from the trochlea of the humerus."
r = matcher.match(text, best_match=True, ignore_syntax=False)
print(r)
