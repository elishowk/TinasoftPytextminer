#  Copyright (C) 2010 elishowk
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


__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner
from tinasoft.pytextminer import filtering
# used for sorting
from operator import itemgetter

import logging
_logger = logging.getLogger('TinaAppLogger')

class Whitelist(PyTextMiner):
    """Whitelist class
    StopNGram edges represent a session's user stopwords
    NGram edges represent a session's whitelisted NGrams
    """

    # standard accept and refuse codes
    refuse = 's'
    accept = 'w'

    def __init__(self, description, id, label, edges=None, **metas):
        if edges is None:
            edges = { 'NGram' : {}, 'StopNGram': {}, 'Normalized': {}, 'Corpus': {} }
        PyTextMiner.__init__(self, description, id, label, edges, **metas)

    def addEdge(self, type, key, value):
        if type == 'Corpus':
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )

    def create(self, storage, periods, filters=None, wlinstance=None):
        """Whitelist creator/updater utility"""
        # initialize ngram cache
        if wlinstance is not None:
            self['edges'] = wlinstance['edges']
        ngrams = {}
        for corpusid in periods:
            # gets a corpus from the storage or continue
            corpusobj = storage.loadCorpus(corpusid)
            if corpusobj is None:
                continue
            # TODO sorts ngrams by occs
            #sortedngrams = reversed(sorted(corpusobj['edges']['NGram'].items(), key=itemgetter(1)))
            # goes over every ngram in the corpus
            for ngid, occ in corpusobj['edges']['NGram'].iteritems():
                # increment edge weight by status
                if ngid in self['edges']['StopNGram']:
                    self.addEdge('StopNGram', ngid, occ)
                    continue
                if ngid in self['edges']['NGram']:
                    self.addEdge('NGram', ngid, occ)
                    continue
                # if NGram's unknown, then loads an checks ngram
                ng = storage.loadNGram(ngid)
                ng['status'] = ''
                # if filtering is activated
                if filters is not None and filtering.apply_filters(ng, filters) is False:
                    ng['status'] = self.refuse
                    self.addEdge('StopNGram', ngid, occ)
                    continue
                # wlinstance NGram edges has the priority on filtering
                if wlinstance is not None:
                    # merging the new whitelist with the complementary one
                    if ngid in wlinstance['edges']['NGram']:
                        ng['status'] = self.accept
                        #occ += wlinstance['edges']['NGrams'][ngid]
                        #self.addEdge( 'NGram', ng['id'], occ )
                        #continue
                    if ngid in wlinstance['edges']['StopNGram']:
                        ng['status'] = self.refuse
                        #occ += wlinstance['edges']['StopNGrams'][ngid]
                        #self.addEdge( 'StopNGram', ng['id'], occ )
                        #continue
                self.addEdge( 'NGram', ngid, occ )
                self.addEdge( 'Normalized', ngid, self['edges']['NGram'][ngid]**len(ng['content']) )
                self.addEdge( 'Corpus', ngid, 1 )
                ngrams[ng['id']] = ng
        return ngrams
