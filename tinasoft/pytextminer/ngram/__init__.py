# -*- coding: utf-8 -*-
# PyTextMiner NGram class
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

class NGram(PyTextMiner):
    """NGram class"""
    def __init__(self, content, id=None, label=None, **metas):
        # normalize
        content = self.normalize(content)
        if label is None:
            label = " ".join(content)
        PyTextMiner.__init__(self, content, id, label, **metas)


class StopNGram(NGram):
    """NGram class"""
    def __init__(self, content, id=None, label=None, **metas):
        # only normalize
        content = self.normalize(content)
        if label is None:
            label = " ".join(content)
        PyTextMiner.__init__(self, content, id, label, **metas)

# WARNING : OBSOLETE !!!
class NGramHelpers():
    """Obsolete"""
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

