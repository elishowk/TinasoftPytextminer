# -*- coding: utf-8 -*-

class Corpora:
    """Corpora contains a list of a corpus"""
    
    def __init__(self, id=None, corpora=None, name=None):
        self.id = id
        # set of corpus' generated unique IDs
        if corpora is None:
            corpora = set([])
        self.corpora = corpora
        # metas
        self.name = name
