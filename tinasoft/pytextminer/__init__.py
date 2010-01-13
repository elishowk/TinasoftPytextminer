# -*- coding: utf-8 -*-
__author__="Julian Bilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:30:11 PM$"

__all__ = ["indexer", "corpora", "corpus", "document", "ngram", "tokenizer", "tagger", "cooccurrences", "stopwords"]

class PyTextMiner:

    """
        PyTextMiner class
        parent of classes analyzed by this module
        common to all analysed objects in this software :
        ngram, document, etc
    """

    def __init__(self, content, id=None, label=None, **metas):
        self.content = content
        if id is None:
            self.id = self.getId( content )
        else:
            self.id = id
        self.label = label
        self.loadOptions( metas )

    def loadOptions(self, metas):
        for attr, value in metas.iteritems():
            setattr(self,attr,value)

    def getId(self, content):
        if type(content).__name__ == 'list':
            return abs( hash( "".join(content) ) )
        elif type(content).__name__ == 'str' or type(content).__name__ == 'unicode':
            return abs( hash( content ) )
        else:
            raise ValueError
            return None

    def normalizeList(self, lst):
        def normalizePOS(lst):
            return lst[0].lower()
        return map( normalizePOS, lst )
