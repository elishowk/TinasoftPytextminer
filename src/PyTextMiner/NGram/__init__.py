# -*- coding: utf-8 -*-
#import hashlib

class NGram():
    """an ngram"""
    def __init__(self, ngram, strRepr, origin=[], occs=None):
        self.ngram = ngram
        self.strRepr = strRepr
        self.origin = origin
        self.occs = occs
        self.id = id( " ".join( ngram ) )

    def __len__(self):
        """ return the length of the ngram"""
        return len(self.ngram)

    def __str__(self):
        return self.strRepr.encode('utf-8')
    
    def __repr__(self):
        return self.strRepr.encode('utf-8')
    
    def __hash__(self):
        return self.id
