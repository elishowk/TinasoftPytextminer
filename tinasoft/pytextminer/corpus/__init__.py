# -*- coding: utf-8 -*-
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

class Corpus(PyTextMiner):
    """a Corpus containing documents"""
    def __init__(self,
            id,
            content=None,
            edges=None,
            **metas):
        if content is None:
            content = id
        PyTextMiner.__init__(self, content, id, id, edges, **metas)

    def addEdge(self, type, key, value):
        # Corpus can link only one time to Document
        if type == 'Document' or type == 'Corpora':
            return self._addUniqueEdge( type, key, value )
        else:
            return  self._addEdge( type, key, value )
