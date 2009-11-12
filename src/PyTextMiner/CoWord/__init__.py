# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 6:32:44 PM$"

class SimpleAnalysis():
    """
    A homemade coword analyzer
    """ 
    def getUniqueNgrams( self, corpus, type, cowords ):
        """
        Lists any coword in a corpus,
        given a PyTextMiner.Corpus
        and target.type selection
        @return all
        """
        all = set()
        for document in corpus.documents:
            for target in document.targets:
                if target.type == type:
                    for ngram in target.ngrams:
                        # TODO init cowords matrix
                        if ngram in all:
                            all[ ngram ].occs += 1
                        else:
                            all.add( ngram )
        return all

    def getCowords( self, corpus, type ):
        cowords = {}
        self.all = self.getUniqueNgrams( corpus, type, cowords )
        #result = self.all
        for searchedngram in self.all:
            #assoc = result
            #del assoc[ representation ]
            #result[ representation ] = assoc
            #print result
            for document in corpus.documents:
#                print document
                for target in document.targets:
#                    print target
                    if target.type == type:
#                        print target.ngrams
                        if searchedngram in target.ngrams:
                            cowords = self.processTarget( target, searchedngram, cowords ) 
        return cowords

    def processTarget( self, target, ngram, cowords ):
        """
        Given a 'ngram' that is present in a 'target'
        test if an item of the 'assoc' dict is in hte 'target's ngrams
        and update coword matrix
        """
        #print target
        #print ngram
        for assocngram in self.all:
            #print assocngram
            key = tuple( sorted( [ ngram, assocngram ] ))
            if assocngram in target.ngrams:
                if cowords.has_key( key ):
                    cowords[ key ] += 1
                else:
                    cowords[ key ] = 1
            #else:
            #    cowords[ key ] = 0
        return cowords
