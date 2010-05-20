# -*- coding: utf-8 -*-
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner
import logging
_logger = logging.getLogger('TinaAppLogger')

class Corpora(PyTextMiner):
    """
    Corpora is a work session
    Corpora contains a list of a corpus
    """

    def __init__(self, name, edges=None, **metas):
        # list of corpus id
        if edges is None:
            edges = {}
        if 'Corpus' not in edges:
            edges['Corpus'] = {}
        PyTextMiner.__init__(self, edges['Corpus'].keys(), name, name, edges=edges, **metas)

    def addEdge(self, type, key, value):
        # Corpora can link only once to a Corpus
        if type == 'Corpus':
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )
