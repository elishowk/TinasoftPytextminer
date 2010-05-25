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
        _logger.debug( wlinstance )
        #if wlinstance is not None:
            #self['edges'] = wlinstance['edges']
        ngrams = {}
        for corpusid in periods:
            # gets a corpus from the storage or continue
            corpusobj = storage.loadCorpus(corpusid)
            if corpusobj is None:
                _logger.error( "corpus %s not found"%corpusid )
                continue
            # TODO sorts ngrams by occs
            #sortedngrams = reversed(sorted(corpusobj['edges']['NGram'].items(), key=itemgetter(1)))
            # goes over every ngram in the corpus
            for ngid, occ in corpusobj['edges']['NGram'].iteritems():
                # if NGram's unknown, then loads an checks ngram
                ng = storage.loadNGram(ngid)
                if ngid is None:
                    _logger.error( "ngram not found %s in corpus %s"%(ngid,corpusid) )
                    continue
                # default status
                ng['status'] = ''
                # if a complementary whitelist is given
                if wlinstance is not None:
                    # increment edge weight by status
                    if ngid in wlinstance['edges']['StopNGram']:
                        ng['status'] = self.refuse
                    if ngid in wlinstance['edges']['NGram']:
                        ng['status'] = self.accept
                # if filtering is active
                if filters is not None and filtering.apply_filters(ng, filters) is False:
                    ng['status'] = self.refuse
                # updates whitelist edges
                if ng['status'] == self.refuse:
                    self.addEdge('StopNGram', ngid, occ)
                    self.addEdge( 'Normalized', ngid, self['edges']['StopNGram'][ngid]**len(ng['content']) )
                else:
                    self.addEdge( 'NGram', ngid, occ )
                    self.addEdge( 'Normalized', ngid, self['edges']['NGram'][ngid]**len(ng['content']) )
                self.addEdge( 'Corpus', ngid, 1 )
                if ng['id'] not in ngrams:
                    ngrams[ng['id']] = ng
                else:
                    ngrams[ng['id']]['status'] = ng['status']
        return ngrams
