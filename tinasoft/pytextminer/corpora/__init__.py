# -*- coding: utf-8 -*-
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

class Corpora(PyTextMiner):
    """Corpora contains a list of a corpus"""

    def __init__(self, name, corpora=None, **metas):
        # list of corpus id
        if corpora is None:
            corpora = {}
        PyTextMiner.__init__(self, corpora, name, name, edges={ 'Corpus': corpora }, **metas)
