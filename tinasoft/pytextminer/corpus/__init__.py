# -*- coding: utf-8 -*-
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

class Corpus(PyTextMiner):
    """a Corpus containing documents"""
    def __init__(self,
            id,
            period_start=None,
            period_end=None,
            documents=None,
            **metas):
        # list of documents unique IDs
        if documents is None:
            documents = {}
        PyTextMiner.__init__(self, documents, id, id, edges={ 'Documents':documents }, **metas)
        self.period_start = period_start
        self.period_end = period_end
