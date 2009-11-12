# -*- coding: utf-8 -*-
class NGram():
    """an ngram"""
    def __init__(self, ngram, occs=None, str=""):
        self.occs = occs
        self.ngram = ngram
        self.strRepr = str
    def __len__(self):
        """ return the length of the ngram"""
        return len(self.ngram)

    def __str__(self):
        return self.strRepr.encode('utf-8')
    def __repr__(self):
        return self.strRepr.encode('utf-8')


