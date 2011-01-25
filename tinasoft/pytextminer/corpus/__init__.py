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

from tinasoft.pytextminer import PyTextMiner

class Corpus(PyTextMiner):
    """a Corpus node representing a group of Documents and NGrams nodes"""
    def __init__(self,
            id,
            content=None,
            edges=None,
            **metas):
        if content is None:
            content = id
        PyTextMiner.__init__(self, content, id, id, edges, **metas)

    def addEdge(self, type, key, value):
        # Corpora is linked only once to Corpus
        if type in ['Corpora','Document']:
            return self._addUniqueEdge( type, key, value )
        else:
            return  self._addEdge( type, key, value )
