# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Oct 20, 2009 6:32:44 PM$"

class SimpleAnalysis():
    """
    A homemade coword analyzer
    """ 
    def analyze( self, corpus, type ):
        cooc = {}
        for document in corpus.documents:
            for target in document.targets:
                if target.type == type:
                    for ng1 in target.ngrams:
                        for ng2 in target.ngrams:
                            #if ng1 < ng2
                            key = tuple( sorted( [ ng1, ng2 ] ))
                            if cooc.has_key( key ):
                                cooc[ key ] += 1
                            else:
                                cooc[ key ] = 1
        return cooc
