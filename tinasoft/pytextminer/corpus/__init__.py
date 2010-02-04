# -*- coding: utf-8 -*-
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

class Corpus(PyTextMiner):
    """a Corpus containing documents"""
    def __init__(self,
            id,
            content=None,
            period_start=None,
            period_end=None,
            **metas):
        # dict of documents {'id' : occurences, ... } in the coprus
        if content is None:
            content={}
        self.period_start = period_start
        self.period_end = period_end
        # content.keys() list can be used as a replacement of id
        PyTextMiner.__init__(self, content.keys(), id, id, edges={ 'Document': content }, **metas)
