# -*- coding: utf-8 -*-
# PyTextMiner NGram class
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

class NGram(PyTextMiner):
    """NGram class"""
    def __init__(self, content, **metas):
        PyTextMiner.__init__(self, content=content, id=None, **metas)


# WARNING : OBSOLETE !!!
class NGramHelpers():
    @staticmethod
    def normalize( ng ):
        def normalizePOS(lst):
            return lst[0].lower()
        return map( normalizePOS, ng )

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

