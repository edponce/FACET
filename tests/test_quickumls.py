from QuickerUMLS import QuickUMLS


text = 'The ulna has dislocated posteriorly from the trochlea of the humerus.'

matcher = QuickUMLS('umls-2018-AA')
# matcher = QuickUMLS('umls-2018-AA-fast')
matches = matcher.match(text, best_match=True, ignore_syntax=False)

print(matches)
