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
from tinasoft.pytextminer import stemmer

import logging
_logger = logging.getLogger('TinaAppLogger')

class NGram(PyTextMiner):
    """NGram class"""
    def __init__(self, tokenlist, id=None, label=None, edges=None, stemmer=None, **metas):
        normalize = self.normalize
        # normalize and stemmer
        if stemmer is not None:
            normalize = lambda x: stemmer.stem(x).lower()
        # normlist will produce an unique id associated with the stemmed form
        normlist = [normalize(word) for word in tokenlist]
        if label is None:
            label = " ".join(tokenlist)
        if edges is None:
            edges = { 'Document' : {}, 'Corpus' : {} }

        PyTextMiner.__init__(self, normlist, id, label, edges, **metas)

    def addEdge(self, type, key, value):
        if type == 'Document':
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )

    def normalize(self, token):
        return token.lower()
