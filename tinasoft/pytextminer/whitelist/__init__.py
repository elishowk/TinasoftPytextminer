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

__author__="elishowk@nonutc.fr"

from tinasoft.pytextminer import PyTextMiner
from tinasoft.pytextminer import ngram
from tinasoft.data import whitelist, Engine

import logging
_logger = logging.getLogger('TinaAppLogger')

import tempfile
from os.path import split


class Whitelist(PyTextMiner,whitelist.WhitelistFile):
    """
    Whitelist class
    StopNGram edges represent a session's user stopwords
    NGram edges represent a session's whitelisted NGrams
    """

    def __init__(self, id, label, edges=None, **metas):
        if edges is None:
            edges = { 'StopNGram': {} }
        # special var storing corpus objects within a whitelist
        self.corpus = {}
        # double heritage
        whitelist.WhitelistFile.__init__(self)
        PyTextMiner.__init__(self, {}, id, label, edges, **metas)
        self.storage = self._get_storage()
        
    def __del__(self):
        del self.storage

    def addEdge(self, type, key, value):
        if type in ['NGram', 'StopNGram']:
            return self._overwriteEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )

    def test(self, ng):
        """
        Identifies an NGram in the whitelist
        """
        if isinstance(ng, ngram.NGram) is True:
            # NG obj submitted
            if ng['id'] in self['edges']['NGram']: return True
            else: return False
        if isinstance(ng, str) is True or isinstance(ng, unicode) is True:
            # NG ID submitted
            if ng in self['edges']['NGram']: return True
            else: return False
        return False

    def _get_storage(self):
        """
        DB connection handler
        one separate database per whitelist
        """
        tmp = tempfile.mkstemp()[1]
        _logger.debug(
            "new connection to a whitelist database at %s"%(tmp)
        )
        options = {'home':".", 'drop_tables': True}
        return Engine("tinasqlite://%s"%tmp, **options)

    def addContent(self, ngram, corpus_id=None, document_id=None):
        """
        inserts or updates a ngram into the whitelist content
        """
        #if status is None:
        #    status = ""
        #ngram["status"] = status
        if corpus_id is not None:
            ngram.addEdge( 'Corpus', corpus_id, 1 )
        if document_id is not None:
            ngram.addEdge( 'Document', document_id, 1 )
        if self.storage.updateManyNGram( ngram ) >= self.storage.MAX_INSERT_QUEUE:
            self.storage.flushNGramQueue()

    def getContent(self, id=None):
        """
        returns a generator of ngrams into the whitelist
        """
        if id is None:
            return self.storage.loadMany("NGram")
        else:
            return self.storage.loadNGram(id)

    def loadFromSession(self, storage, corpora, periods=None):
        """
        Whitelist creator utility
        Loads a whitelist from a session storage
        """
        if periods is None:
            periods = corpora['edges']['Corpus'].keys()
        for corpusid in periods:
            # gets a corpus from the storage or continue
            corpusobj = storage.loadCorpus(corpusid)
            if corpusobj is None:
                _logger.error( "corpus %s not found"%corpusid )
                continue

            # updates whitelist's corpus to prepare export
            if  corpusObj['id'] not in self['corpus']:
                self['corpus'][corpusObj['id']] = corpusObj

            # occ is the number of docs in the corpus where ngid appears
            for ngid, occ in corpusobj['edges']['NGram'].iteritems():
                ng = storage.loadNGram(ngid)
                for docid in ng['edges']['Document'].keys():
                    if docid in corpusObj['edges']['Document'].keys():
                        self.addContent( ng, corpusObj['id'], docid )
                self.addEdge("NGram", ng['id'], occ)
            self.storage.flushNGramQueue()