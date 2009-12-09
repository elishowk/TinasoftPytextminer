# -*- coding: utf-8 -*-
__author__="Elias Showk"

class Corpus:
    """a Corpus containing documents"""
    def __init__(self, name, period_start=None, period_end=None, documents=None):
        self.name = name
        # list of documents unique IDs
        if documents is None: 
            documents = []
        self.documents = documents
        self.period_start = period_start
        self.period_end = period_end
#    def __str__(self):
#        return self.name
#
#    def __repr__(self):
#        return "<%s>"%self.name

