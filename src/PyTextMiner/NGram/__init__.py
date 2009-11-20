# -*- coding: utf-8 -*-
# PyTextMiner NGram class

__author__="Elias Showk"

class NGram(dict):
    """an ngram"""
    def __init__(self, ngram, str, original=[], occs=None, id=None):
        if id is None:
            self.id = abs(hash("".join(ngram)))
        else:
            self.id = id
        dict.__init__(
                self,
                ngram = ngram,
                str = str,
                original = original,
                occs = occs
        )

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
