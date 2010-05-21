# -*- coding: utf-8 -*-
# PyTextMiner NGram class
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

import logging
_logger = logging.getLogger('TinaAppLogger')

class Whitelist(PyTextMiner):
    """Whitelist class"""
    def __init__(self, description, id, label, edges=None, **metas):
        if edges is None:
            edges = { 'NGram' : {} }
        PyTextMiner.__init__(self, description, id, label, edges, **metas)

    def addEdge(self, type, key, value):
            return self._addEdge( type, key, value )
