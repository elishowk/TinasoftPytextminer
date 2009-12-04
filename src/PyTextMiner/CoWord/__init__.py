# -*- coding: utf-8 -*-
__author__="Elias Showk"
from itertools import *

class SimpleAnalysis():
    """
    A homemade coword analyzer
    """ 
    def job( self, store, documentNum, ng1, ng2 ):
        docngrams = store.fetchDocumentNGramID( documentNum )
        if ng1 in docngrams and ng2 in docngrams:
            return 1
        else:
            return 0

    def process( self, store, corpusID ): 
        ngrams = store.fetchCorpusNGramID( corpusID )
        documents = store.fetchCorpusDocumentID( corpusID )
        ngpairs = combinations( ngrams, 2 )
        #print len(ngpairs)
        cooc = []
        for pair in ngpairs:
            for doc in documents:
                cooc.append( ( corpusID, pair[0], pair[1], self.job( store, doc, pair[0], pair[1] ) ) )
        print cooc
