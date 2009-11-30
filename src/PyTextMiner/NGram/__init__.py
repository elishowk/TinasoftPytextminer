# -*- coding: utf-8 -*-
# PyTextMiner NGram static helpers class

__author__="Elias Showk"

class NGram():
    pass

class NGramHelpers():
    """ngram static method helpers"""

    @staticmethod
    def normalize( ng ):
        def normalizePOS(lst):
            return lst[0].lower()
        return map( normalizePOS, ng )

    @staticmethod
    def generateID( lst ):
        return abs(hash("".join( lst )))

    @staticmethod
    def filterUnique( rawDict, threshold, corpusNum, mapping=None ):
        delList = []
        filteredDict = {}
        assocNGramCorpus = []
        for ngid in rawDict.keys():
            if rawDict[ ngid ]['occs'] < threshold:
                del rawDict[ ngid ]
                delList.append( ngid )
            else:
                assocNGramCorpus.append( ( ngid, corpusNum, rawDict[ ngid ]['occs'] ) )
                #del rawDict[ ngid ]['occs']
                if mapping is not None:
                    item = mapping( rawDict[ ngid ] )
                else:
                    item = rawDict[ ngid ]
                filteredDict[ ngid ] = item

        return ( filteredDict, delList, assocNGramCorpus )

