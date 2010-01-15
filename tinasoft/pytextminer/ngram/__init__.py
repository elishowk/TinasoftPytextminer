# -*- coding: utf-8 -*-
# PyTextMiner NGram class
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

class NGram(PyTextMiner):
    """NGram class"""
    def __init__(self, content, id=None, label=None, **metas):
        # normalize and separate postag
        content = self.normalize(content)
        content = map( lambda tok: tok[0], content )
        if label is None:
            label = " ".join(content)
        PyTextMiner.__init__(self, content, id, label, **metas)

    def normalize( self, ng ):
        def normalizePOS(lst):
            return lst[0].lower()
        return map( normalizePOS, ng )

class StopNGram(NGram):
    """NGram class"""
    def __init__(self, content, id=None, label=None, **metas):
        # only normalize
        content = self.normalize(content)
        if label is None:
            label = " ".join(content)
        PyTextMiner.__init__(self, content, id, label, **metas)

    def normalize( self, ng ):
        return [x.lower() for x in ng]

# WARNING : OBSOLETE !!!
class NGramHelpers():

    @staticmethod
    def filterUnique( rawDict, threshold, corpusNum, sqliteEncode ):
        delList = []
        filteredDict = {}
        assocNGramCorpus = []
        for ngid in rawDict.keys():
            if rawDict[ ngid ].occs < threshold:
                del rawDict[ ngid ]
                delList.append( ngid )
            else:
                assocNGramCorpus.append( ( ngid, corpusNum, rawDict[ ngid ].occs ) )
                item = rawDict[ ngid ]
                filteredDict[ ngid ] = sqliteEncode(item)
        return ( filteredDict, delList, assocNGramCorpus )

