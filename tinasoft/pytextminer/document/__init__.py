# -*- coding: utf-8 -*-
__author__="Elias Showk"

import datetime
from tinasoft.pytextminer import PyTextMiner

class Document(PyTextMiner):
    """a Document containing targets containing ngrams"""

    def __init__(
            self,
            content,
            id,
            title,
            edges=None,
            datestamp=None,
            author=None,
            ngramMin=1,
            ngramMax=3,
            forbChars="[^a-zA-Z\s\@ÂÆÇÈÉÊÎÛÙàâæçèéêîĨôÔùûü\,\.\;\:\!\?\"\'\[\]\{\}\(\)\<\>]",
            ngramSep= u"[\s]+",
            ngramEmpty = " ",
            **metas
        ):

        PyTextMiner.__init__(self, content, id, title, edges, **metas)
        self.date = datestamp
        self.author = author
        # these are tokenization paramaters
        self.ngramMin = ngramMin
        self.ngramMax = ngramMax
        self.forbChars = forbChars
        self.ngramSep = ngramSep
        self.ngramEmpty = ngramEmpty

    #def getDate(self):
    #    if self.date is not None:
    #        return self.date.split('/')

    def addEdge(self, type, key, value):
        if type in ["Document","NGram","Corpus"]:
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )
