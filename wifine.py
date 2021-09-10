import os
import pickle
from typing import Optional
from tqdm import tqdm


###############################################################################
# Fine Entity Parsing
###############################################################################

def parse_category(named_entity_type: str, level: int) -> Optional[str]:
    split = named_entity_type.split('/')
    if len(split) >= level + 1:
        return split[level]
    else:
        return None


def coarse_type(named_entity_type: str) -> Optional[str]:
    return parse_category(named_entity_type, 1)


def fine_type(named_entity_type: str) -> Optional[str]:
    return parse_category(named_entity_type, 2)


def ultra_fine_type(named_entity_type: str) -> Optional[str]:
    return parse_category(named_entity_type, 3)


###############################################################################
# Classes
###############################################################################

class Vocab:

    def __init__(self, path):
        self._path = path

        with open(path, 'r') as f:
            vocab = []
            
            for line in f:
                word_type = line.split()[0]
                vocab.append(word_type)

            self._vocab = vocab
            
        self._coarse_types = None

    def __getitem__(self, x):
        return self._vocab[x]

    def __len__(self):
        return len(self._vocab)
    
    def coarse_types(self):
        if self._coarse_types:
            return self._coarse_types
        
        self._coarse_types = { coarse_type(t) for t in self._vocab }
        return self._coarse_types
    
    def subtypes(self, t):
        return [x for x in self._voccab if x.startswith(t) and x != t]


class Index:

    """
    A map from document id to directory id
    """

    def __init__(self, path, cache_path=None):
        self._path = path
        self._docid_to_dirid = {}

        if cache_path:
            try:
                self._load(cache_path)
            except:
                self._parse()
                self._save(cache_path)
        else:
            self._parse()

    def _load(self, cache_path):
        with open(cache_path, 'rb') as f:
            self._docid_to_dirid = pickle.load(f)

    def _parse(self):
        dir_files = os.listdir(self._path)
        for dir_file in tqdm(dir_files):
            dirid = int(dir_file)

            with open(os.path.join(self._path, dir_file)) as f:
                for line in f:
                    s = line.split()

                    if s[0] == 'ID':
                        docid = int(s[1])
                        self._docid_to_dirid[docid] = dirid

    def _save(self, cache_path):
        with open(cache_path, 'wb') as f:
            pickle.dump(self._docid_to_dirid, f)

    def __getitem__(self, docid):
        return self._docid_to_dirid[docid]

    def __len__(self):
        return len(self._docid_to_dirid)

    def all_docids(self):
        return list(self._docid_to_dirid.keys())

    def all_dirids(self):
        return list(set(self._docid_to_dirid.values()))

    def get_directory(self, dirid):
        # implement in subclass
        pass

    def all_directories(self):
        for dirid in self.all_dirids():
            yield self.get_directory(dirid)

    def get_document(self, docid):
        directory = self.get_directory(self[docid])
        for document in directory.all_documents():
            if document.docid == docid:
                return document

    def all_documents(self):
        for dirid in tqdm(self.all_dirids()):
            for document in self.get_directory(dirid).all_documents():
                yield document


class Directory:

    def all_documents(self):
        # implement in subclass
        pass


###############################################################################
# Document
###############################################################################

class DocumentIndex(Index):

    def get_directory(self, dirid):
        dir_file = str(dirid)
        return DocumentDirectory(dirid, os.path.join(self._path, dir_file))


class DocumentDirectory(Directory):

    def __init__(self, dirid, path):
        self.dirid = dirid
        self._path = path
        
    def all_documents(self):
        with open(self._path) as f:
            docid = None
            lines = None

            for line in f:
                s = line.split()
                if s[0] == 'ID':
                    if docid and lines:
                        yield Document(docid , self._ids_from_lines(lines))

                    docid = int(s[1])
                    lines = []
                else:
                    lines.append(line)

            if docid and lines:
                yield Document(docid, self._ids_from_lines(lines))

    def _ids_from_lines(self, lines):
        return list(map(lambda line: list(map(int, line.split())), lines))


class Document:

    def __init__(self, docid, sentences_as_ids):
        self.docid = docid
        self._sentences_as_ids = sentences_as_ids

    def _lazy(self, varname, gen):
        try:
            return getattr(self, varname)
        except:
            val = gen()
            setattr(self, varname, val)
            return val

    def sentences_as_ids(self):
        return self._sentences_as_ids

    def sentences_as_tokens(self) -> list[list[str]]:
        def thunk():
            return list(map(
                lambda sentence: [DOCUMENT_VOCAB[vocab_id] for vocab_id in sentence], 
                self._sentences_as_ids
            ))

        return self._lazy('_sentences_as_tokens', thunk)

    def fine_entities(self):
        def thunk():
            try:
                return FINE_ENTITY_INDEX.get_document(self.docid)
            except:
                return None

        return self._lazy('_fine_entities', thunk)


###############################################################################
# Fine Entity
###############################################################################

class FineEntityIndex(Index):

    def get_directory(self, dirid):
        dir_file = str(dirid)
        return FineEntityDirectory(dirid, os.path.join(self._path, dir_file))


class FineEntityDirectory(Directory):

    def __init__(self, dirid, path):
        self.dirid = dirid
        self._path = path
        
    def all_documents(self):
        with open(self._path) as f:
            docid = None
            lines = None

            for line in f:
                s = line.split()
                if s[0] == 'ID':
                    if docid and lines:
                        yield FineEntities(docid, **self._properties_from_lines(lines))

                    docid = int(s[1])
                    lines = []
                else:
                    lines.append(line)

            if docid and lines:
                yield FineEntities(docid, **self._properties_from_lines(lines))

    def _properties_from_lines(self, lines):
        sent_idx = []
        begin = []
        end = []
        men_type = []
        figer_types = []
        gillick_types = []

        for line in lines:
            s = line.split('\t')
            sent_idx.append(int(s[0]))
            begin.append(int(s[1]))
            end.append(int(s[2]))
            men_type.append(int(s[3]))
            figer_types.append(list(map(int, s[4].split())) if s[4] != '-' else [])
            gillick_types.append(list(map(int, s[5].split())) if s[5] != '-\n' else [])

        return {
            'sent_idx': sent_idx,
            'begin': begin,
            'end': end,
            'men_type': men_type,
            'figer_types': figer_types,
            'gillick_types': gillick_types
        }


class FineEntities:

    def __init__(self, docid, sent_idx, begin, end, men_type, figer_types, gillick_types):
        self.docid = docid
        self.sent_idx = sent_idx
        self.begin = begin
        self.end = end
        self.men_type = men_type
        self.figer_types = figer_types
        self.gillick_types = gillick_types

    def rows(self):
        return zip(self.sent_idx, self.begin, self.end, self.men_type, self.figer_types, self.gillick_types)


###############################################################################
# Globals
###############################################################################

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCUMENTS_DIR = os.path.join(SCRIPT_DIR, 'Documents/')
FINENE_DIR = os.path.join(SCRIPT_DIR, 'FineNE/')

DOCUMENT_VOCAB = Vocab(os.path.join(DOCUMENTS_DIR, 'document.vocab'))
FIGER_VOCAB = Vocab(os.path.join(FINENE_DIR, 'figer.vocab'))
GILLICK_VOCAB = Vocab(os.path.join(FINENE_DIR, 'gillick.vocab'))

DOCUMENT_INDEX = DocumentIndex(
    os.path.join(DOCUMENTS_DIR, 'Documents'), 
    cache_path=os.path.join(SCRIPT_DIR, 'documents.index')
)
FINE_ENTITY_INDEX = FineEntityIndex(
    os.path.join(FINENE_DIR, 'FineEntity'), 
    cache_path=os.path.join(SCRIPT_DIR, 'fineentity.index')
)

