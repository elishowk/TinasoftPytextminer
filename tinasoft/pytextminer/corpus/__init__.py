# -*- coding: utf-8 -*-
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

class Corpus(PyTextMiner):
    """a Corpus containing documents"""
    def __init__(self, 
            name,
            period_start=None,
            period_end=None,
            documents=None,
            **metas):
        # name is the primary key
        self.name = name
        # list of documents unique IDs
        if documents is None: 
            documents = []
        self.documents = documents
        PyTextMiner.__init__(self, content=self.documents, id=self.name, **metas)
        self.period_start = period_start
        self.period_end = period_end
