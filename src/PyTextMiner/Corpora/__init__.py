# -*- coding: utf-8 -*-

__author__="Elias Showk"

class Corpora:
    """Corpora contains a list of a corpus"""
    
    def __init__(self, corpora=None, name=None):
        # set of corpus' generated unique IDs
        if corpora is None:
            corpora = set([])
        self.corpora = corpora
        # metas
        self.name = name
