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
from tinasoft.pytextminer import PyTextMiner, corpus, ngram

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
        if type in ["Document","keyword"]:
            return self._overwriteEdge( type, key, value )
#        elif type in ['Corpus','NGram']:
#            return self._addUniqueEdge( type, key, value )
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
                        self._updateNGramCorpusEdge(storage, targetid, -1)

    def _updateNGramCorpusEdge(self, storage, ngid, incrvalue):
        """
        decrement or increment the Corpus-NGram edges, symmetrically
        """
        incrementedges = {
            "NGram": {
                ngid : incrvalue
            }
        }
        # updates Corpus-NGram edges, symmetrically ONLY if exists
        for corpus_id in self['edges']['Corpus'].keys():
            updateCorpus = corpus.Corpus(corpus_id, edges=incrementedges)
            storage.updateCorpus(updateCorpus, redondantupdate=True)


    def deleteNGramForm(self, form, ngid, is_keyword=False):
        """
        Removes a NGram form, optionally a keyword
        And _cleanEdges
        """
        matched = 0
        for target in self['target']:
            matched += len(re.findall(r"\b%s\b"%form,self[target], re.I|re.U))
        # decrement Document-NGram with count + redondant
        self._addEdge("NGram", ngid, -matched)
        # if removing a keyword
        if is_keyword is True and form in self.edges["keyword"]:
             del self.edges["keyword"][form]

    def addNGramForm(self, ngObj, storage, is_keyword=False):
        """
        Adds a NGram form, optionally a keyword
        And updates Corpus if needed
        """
        ngid = ngObj.id
        if ngid in self['edges']['NGram']:
            _logger.warning("keyword %s is already in document %s, aborting"%(ngObj['label'], self.id))
            return 0
        occs = 0
        for form in ngObj['edges']['label'].keys():
            for target in self['target']:
                occs += len(re.findall(r"\b%s\b"%form,self[target], re.I|re.U))
        # if it occurs
        if occs > 0:
            # if it's new
            if ngid not in self.edges['NGram']:
                self._updateNGramCorpusEdge(storage, ngid, 1)
            self.addEdge("NGram", ngid, occs)
        # if not in the content, add both keyword and NGram links
        elif is_keyword is True:
            # and add a Document-keyword link
            self.addEdge("keyword", form, ngid)
            self.addEdge("NGram", ngid, 1)
            self._updateNGramCorpusEdge(storage, ngid, 1)
            occs = 1
        if occs > 0:
            # increments NGram-Document edges
            new_ngram = ngram.NGram(ngObj['content'], label=ngObj['label'])
            new_ngram.addEdge("Document", self.id, occs)
            storage.updateNGram(new_ngram, False)
        return occs
