from wifine import *

print('Document IDs')
doc_ids = DOCUMENT_INDEX.all_docids()
print(str(doc_ids)[:75], '...')

print()
print('Document 6534091')
d = DOCUMENT_INDEX.get_document(6534091)
print(str(d.sentences_as_ids())[:75], '...')
print(str(d.sentences_as_tokens())[:75], '...')
for sent_idx, begin, end, men_type, figer_types, gillick_types in d.fine_entities().rows():
    print(sent_idx, begin, end, men_type, figer_types, gillick_types)

print()
print('Figer/Gillick types')
print(len(FIGER_VOCAB))
print(len(GILLICK_VOCAB))
print(FIGER_VOCAB[25])
print(GILLICK_VOCAB[25])

print()
print('Parsing /other/product/camera')
print(coarse_type('/other/product/camera'))
print(fine_type('/other/product/camera'))
print(ultra_fine_type('/other/product/camera'))

