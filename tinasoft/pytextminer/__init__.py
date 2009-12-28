# -*- coding: utf-8 -*-
__author__="Julian Bilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:30:11 PM$"

__all__ = ["corpora", "corpus", "document", "ngram", "tokenizer", "tagger", "coword", "stopwords"]


"""
    PyTextMiner class
    parent of classes analyzed by this module
    common to all analysed objects in this software :
    ngram, document, etc
"""
class PyTextMiner:

    """ defaults """
    options = { }

    def __init__(self, content, id=None, **metas):
        self.content = content
        if id is None:
            self.id = self.getId( content )
        else:
            self.id = id
        self.loadOptions( metas )

    # TODO dict getter (for blob storage in tinasoft.data)

    def loadOptions(self, options):
        self.options.update(options)
        for attr, value in self.options.iteritems():
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
        return map( normalizePOS, ng )
