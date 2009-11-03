# -*- coding: utf-8 -*-
class NGram():
    """an ngram"""
    def __init__(self, ngram, occs=None):
        self.occs = occs
        self.ngram = ngram
    def __len__(self):
        """ return the length of the ngram"""
        return len(self.ngram)
