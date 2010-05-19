# -*- coding: utf-8 -*-
# PyTextMiner NGram class
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

import logging
_logger = logging.getLogger('TinaAppLogger')

class NGram(PyTextMiner):
    """NGram class"""
    def __init__(self, tokenlist, id=None, label=None, edges=None, **metas):
        # normalize
        tokenlist = self.normalize(tokenlist)
        if label is None:
            label = " ".join(tokenlist)
        if edges is None:
            edges = { 'Document' : {}, 'Corpus' : {} }
        PyTextMiner.__init__(self, tokenlist, id, label, edges, **metas)

    def addEdge(self, type, key, value):
        if type == 'Document':
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )