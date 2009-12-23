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

    # TODO dict getter (for blob storage in tinasoft.data)

    def loadOptions(self, options):
        if options is not None:
            for attr, value in options.iteritems():
                setattr(attr,value)

    def getId(self, root):
        if type(root).__name__ == 'list':
            return abs( hash( "".join(root) ) )
        elif type(root).__name__ == 'str' or type(root).__name__ == 'unicode':
            return abs( hash( root ) )
        else:
            raise ValueError
            return None

    def normalizeList(self, lst):
        def normalizePOS(lst):
            return lst[0].lower()
        return map( normalizePOS, ng )
