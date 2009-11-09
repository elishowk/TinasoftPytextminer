# -*- coding: utf-8 -*-
class Corpora:
    """Corpora contains a list of a corpus"""
    
    def __init__(self, corpora=None):
        if corpora is None:
            self.corpora = []
        self.corpora = corpora
