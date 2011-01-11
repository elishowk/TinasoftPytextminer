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
