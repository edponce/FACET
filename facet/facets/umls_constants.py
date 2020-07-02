# NOTE: UMLS headers should be automatically parsed from UMLS MRFILES.RRF.
HEADERS_MRCONSO = (
    'cui', 'lat', 'ts', 'lui', 'stt', 'sui', 'ispref', 'aui', 'saui',
    'scui', 'sdui', 'sab', 'tty', 'code', 'str', 'srl', 'suppress', 'cvf'
)

# NOTE: UMLS headers should be automatically parsed from UMLS MRFILES.RRF.
HEADERS_MRSTY = (
    'cui', 'sty', 'hier', 'desc', 'sid', 'num'
)

ACCEPTED_SEMTYPES = {
    'T029': 'Body Location or Region',
    'T023': 'Body Part, Organ, or Organ Component',
    'T031': 'Body Substance',
    'T060': 'Diagnostic Procedure',
    'T047': 'Disease or Syndrome',
    'T074': 'Medical Device',
    'T200': 'Clinical Drug',
    'T203': 'Drug Delivery Device',
    'T033': 'Finding',
    'T184': 'Sign or Symptom',
    'T034': 'Laboratory or Test Result',
    'T058': 'Health Care Activity',
    'T059': 'Laboratory Procedure',
    'T037': 'Injury or Poisoning',
    'T061': 'Therapeutic or Preventive Procedure',
    'T048': 'Mental or Behavioral Dysfunction',
    'T046': 'Pathologic Function',
    'T121': 'Pharmacologic Substance',
    'T201': 'Clinical Attribute',
    'T130': 'Indicator, Reagent, or Diagnostic Aid',
    'T195': 'Antibiotic',
    'T039': 'Physiologic Function',
    'T040': 'Organism Function',
    'T041': 'Mental Process',
    'T170': 'Intellectual Product',
    'T191': 'Neoplastic Process'
}

# Mapping between UMLS:spaCy languages.
LANGUAGES = {
    'ENG': 'en',  # English
    'GER': 'de',  # German
    'SPA': 'es',  # Spanish
    'POR': 'pt',  # Portuguese
    'FRE': 'fr',  # French
    'ITA': 'it',  # Italian
    'DUT': 'nl'   # Dutch
}
