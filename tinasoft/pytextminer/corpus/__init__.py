# -*- coding: utf-8 -*-
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

class Corpus(PyTextMiner):
    """a Corpus containing documents"""
    def __init__(self,
            id,
            edges=None,
            period_start=None,
            period_end=None,
            **metas):
        # dict of documents {'id' : occurences, ... } in the coprus
        if edges is None:
            edges={}
        if 'Document' not in edges:
            edges['Document'] = {}
        self.period_start = period_start
        self.period_end = period_end
        # content.keys() list can be used as a replacement of id
        PyTextMiner.__init__(self, edges['Document'].keys(), id, id, edges=edges, **metas)

    def addEdge(self, type, key, value):
        #_logger.debug(key)
        if type == 'Document':
            return self._addUniqueEdge( type, key, value )
        else:
            return  self._addEdge( type, key, value )

