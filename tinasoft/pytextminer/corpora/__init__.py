# -*- coding: utf-8 -*-
__author__="Elias Showk"

class Corpora:
    """Corpora contains a list of a corpus"""
    
    def __init__(self, name, corpora=None):
        # by convention, name is a primary key
        self.name = name
        # list of corpus id
        if corpora is None:
            corpora = []
        self.corpora = corpora
        
    def __str__(self):
        return self.name
