# -*- coding: utf-8 -*-
import hashlib

class NGram():
    """an ngram"""
    def __init__(self, ngram, occs=None, strRepr=''):
        self.occs = occs
        self.ngram = ngram
        self.strRepr = strRepr
    def __len__(self):
        """ return the length of the ngram"""
        return len(self.ngram)

    def __str__(self):
        return self.strRepr.encode('utf-8')
    def __repr__(self):
        return self.strRepr.encode('utf-8')
    def __hash__(self):
        return id(self.strRepr)
