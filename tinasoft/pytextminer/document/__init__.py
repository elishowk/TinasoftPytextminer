# -*- coding: utf-8 -*-
__author__="Elias Showk"

import datetime
from tinasoft.pytextminer import PyTextMiner, corpus

import re

class Document(PyTextMiner):
    """a Document node containing content and linkied to ngrams and corpus"""

    def __init__(
            self,
            content,
            id,
            label,
            edges=None,
            **metas
        ):
        PyTextMiner.__init__(self, content, id, label, edges, **metas)

    def addEdge(self, type, key, value):
        if type in ["Corpus"]:
            return self._addUniqueEdge( type, key, value )
        elif type in ["Document"]:
            return self._overwriteEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )
            
    def _cleanEdges(self, storage, **kwargs):
        for targettype in self['edges'].keys():
            for targetid in self['edges'][targettype].keys():
                if self['edges'][targettype][targetid] == 0:
                    del self['edges'][targettype][targetid]
                    if targettype == "NGram":
                        # decrement NGram-Corpus edges
                        edges = {
                            "NGram": {
                                targetid : -1
                            }
                        }
                        for corpus_id in self['edges']['Corpus'].keys():
                            updateCorpus = corpus.Corpus(corpus_id, edges=egdes)
                            storage.updateCorpus(updateCorpus, redondantupdate=True)
                            
    def deleteNGramForm(self, form, ngid, storage):
        # count occurrences of the form in self['content']
        matched = re.findall(r"\b%s\b"%form,self['content'],re.I+re.U)
        # decrement Document-NGram with count + redondant
        self.addEdge("NGram", ngid, -len(matched))
        self._cleanEdges(storage)
