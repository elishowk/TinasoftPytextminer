# -*- coding: utf-8 -*-
class Corpora:
    """Corpora contains a list of a corpus"""
    
    def __init__(self, corpora=None, id=None):
        if corpora is None:
            corpora = []
        self.corpora = corpora
        self.id = id
