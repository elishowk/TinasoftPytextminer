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
from tinasoft.pytextminer import filtering
from tinasoft.pytextminer import ngram
from tinasoft.data import whitelist

# used for sorting
#from operator import itemgetter

import logging
_logger = logging.getLogger('TinaAppLogger')

class Whitelist(PyTextMiner,whitelist.WhitelistFile):
    """
    Whitelist class
    StopNGram edges represent a session's user stopwords
    NGram edges represent a session's whitelisted NGrams
    """

    # standard accept and refuse codes
    #refuse = 's'
    #accept = 'w'

    def __init__(self, id, label, content=None, edges=None, **metas):
        if content is None:
            content = {}
        if edges is None:
            edges = { 'NGram' : {}, 'StopNGram': {} }
        self.corpus = {}
        whitelist.WhitelistFile.__init__(self)
        PyTextMiner.__init__(self, content, id, label, edges, **metas)

    def addEdge(self, type, key, value):
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
