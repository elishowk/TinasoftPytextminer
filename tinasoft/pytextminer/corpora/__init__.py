# -*- coding: utf-8 -*-
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

__author__="elishowk@nonutc.fr"

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
        content = []
        if edges is not None and 'Corpus' in edges:
            content = edges['Corpus'].keys()
        PyTextMiner.__init__(self, content, name, name, edges=edges, **metas)

    def addEdge(self, type, key, value):
        # Corpora can link only once to a Corpus
        if type == 'Corpus':
            return self._addUniqueEdge( type, key, value )
        elif type in ['Whitelist','Source']:
            return self._overwriteEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )
