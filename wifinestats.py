from wifine import *
from tqdm import tqdm
import itertools

total_figer_types = [0] * len(FIGER_VOCAB)
total_gillick_types = [0] * len(GILLICK_VOCAB)

encountered = set()
duplicates = set()
for fe in tqdm(FINE_ENTITY_INDEX.all_documents()):
    if fe.docid in encountered:
        duplicates.add(fe.docid)
        continue
    encountered.add(fe.docid) 

    for types in fe.figer_types:
        for t in types:
            total_figer_types[t] += 1
    for types in fe.gillick_types:
        for t in types:
            total_gillick_types[t] += 1

def print_results(total_types, vocab):
    total_types = [(count, vocab[i]) for i, count in enumerate(total_types)]
    total_types.sort(key=lambda x: x[0], reverse=True)
    for (count, desc) in total_types:
        print(desc, count)

print('FIGER')
print_results(total_figer_types, FIGER_VOCAB)
print()
print('GILLICK')
print_results(total_gillick_types, GILLICK_VOCAB)
print()
print('DUPLICATES')
print(duplicates)


