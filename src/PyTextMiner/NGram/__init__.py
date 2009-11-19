# -*- coding: utf-8 -*-
# PyTextMiner NGram class

class NGram(dict):
    """an ngram"""
    def __init__(self, ngram, str, original=[], occs=None):
        dict.__init__(self, ngram=ngram, str=str, occs=occs )

#    def __len__(self):
#        """length of the ngram"""
#        return len(self.ngram)

#    def __str__(self):
#        return self.strRepr.encode('utf-8','replace')
#    
#    def __repr__(self):
#        return self.strRepr.encode('utf-8','replace')
#    
#    def __hash__(self):
#        return self.id
