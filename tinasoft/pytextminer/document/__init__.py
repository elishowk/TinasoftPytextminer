# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__ = "elishowk@nonutc.fr"
import datetime
import re
from tinasoft.pytextminer import PyTextMiner, corpus

import logging
_logger = logging.getLogger('TinaAppLogger')

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
        if type in ["Document"]:
            return self._overwriteEdge( type, key, value )
        elif type in ['Corpus']:
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )
            
    def _cleanEdges(self, storage, **kwargs):
        for targettype in self['edges'].keys():
            for targetid in self['edges'][targettype].keys():
                if self['edges'][targettype][targetid] == 0:
                    del self['edges'][targettype][targetid]
                    if targettype == "NGram":
                        # decrement NGram-Corpus edges
                        decrementedges = {
                            "NGram": {
                                targetid : -1
                            }
                        }
                        for corpus_id in self['edges']['Corpus'].keys():
                            updateCorpus = corpus.Corpus(corpus_id, edges=decrementedges)
                            storage.updateCorpus(updateCorpus, redondantupdate=True)
                            
    def deleteNGramForm(self, form, ngid, storage):
        # count occurrences of the form in self['content']
        matched = re.findall(r"\b%s\b"%form,self['content'],re.I | re.U)
        # decrement Document-NGram with count + redondant
        self.addEdge("NGram", ngid, -len(matched))
        #_logger.debug(self.edges['NGram'])
        self._cleanEdges(storage)
