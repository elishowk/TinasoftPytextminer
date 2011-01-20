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
        if 'keyword' not in self.edges:
            self.edges['keyword']={}

    def addEdge(self, type, key, value):
        if type in ["Document","NGram","keyword"]:
            return self._overwriteEdge( type, key, value )
        elif type in ['Corpus']:
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )

    def _cleanEdges(self, storage, **kwargs):
        for targettype in self['edges'].keys():
            for targetid in self['edges'][targettype].keys():
                if self['edges'][targettype][targetid] <= 0:
                    del self['edges'][targettype][targetid]
                    # special trigger for NGram-Corpus edge
                    if targettype == "NGram":
                        # decrement the edge
                        decrementedges = {
                            "NGram": {
                                targetid : -1
                            }
                        }
                        for corpus_id in self['edges']['Corpus'].keys():
                            updateCorpus = corpus.Corpus(corpus_id, edges=decrementedges)
                            ### CHECK : no need for redondantupdate ??
                            storage.updateCorpus(updateCorpus, redondantupdate=False)

    def deleteNGramForm(self, form, ngid, storage, is_keyword=False):
        """
        Removes a NGram form, optionally a keyword
        And propagates changes to neighbours edges
        """
        matched = 0
        for target in self['target']:
            matched += len(re.findall(r"\b%s\b"%form,self[target], re.I|re.U))
        # decrement Document-NGram with count + redondant
        self.addEdge("NGram", ngid, -matched)
        # if removing a keyword
        if is_keyword is True and form in self.edges["keyword"]:
             del self.edges["keyword"][form]
        self._cleanEdges(storage)

    def addNGramForm(self, form, ngid, storage, is_keyword=False):
        """
        Adds a NGram form, optionally a keyword
        And propagates changes to neighbours edges
        """
        occs = 0
        for target in self['target']:
            occs += len(re.findall(r"\b%s\b"%form,self[target], re.I|re.U))
        # if adding a keyword
        if is_keyword is True:
             self.addEdge("keyword", form, ngid)
             # force to count one occurrence if is_keyword
             if occs == 0:
                occs = 1
        # increment the Corpus-NGram edge
        if ngid not in self.edges['NGram'] and occs > 0:
            incrementedges = {
                "NGram": {
                    ngid : 1
                }
            }
            for corpus_id in self['edges']['Corpus'].keys():
                updateCorpus = corpus.Corpus(corpus_id, edges=incrementedges)
                storage.updateCorpus(updateCorpus, redondantupdate=False)
            self.addEdge("NGram", ngid, occs)
        else:
            _logger.debug("NGram %s is already indexed into Document %s"%(ngid, self.id))
        return occs
