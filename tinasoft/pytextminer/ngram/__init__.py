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
    def __init__(self, tokenlist, id=None, label=None, edges=None, stemmer=None, postag=None, **metas):
        """
        initiate the object
        normalize must be local value for pickling reasons
        """
        normalize = self.normalize
        # normalize and stemmer
        if stemmer is not None:
            normalize = lambda x: stemmer.stem(x).lower()
        # normlist will produce an unique id associated with the stemmed form
        normlist = [normalize(word) for word in tokenlist]
        # auto creates label
        if label is None:
            label = " ".join(tokenlist)
        postag_label = None
        # prepares postag
        if postag is not None:
            postag_label = " ".join(postag)
            metas["postag"] = postag

        if edges is None:
            edges = { 'Document' : {}, 'Corpus' : {}, 'label': {}, 'postag' : {}}
        PyTextMiner.__init__(self, normlist, id, label, edges, **metas)
        self.updateMajorForm(label, postag_label)

    def addEdge(self, type, key, value):
        """
        The NGram object accepts only one edge write to a Document object
        All the ohter edges are multiples
        """
        if type == 'Document':
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )

    def normalize(self, token):
        """
        Default tokens normalizing
        """
        return token.lower()

    def updateMajorForm(self, label, postag_label):
        """
        updates major form of a nlemma
        """
        # updates edges
        self.addEdge('label', label, 1)
        if postag_label is not None:
            self.addEdge('postag', postag_label, 1)
        # updates major form label attr
        self.label = self.getLabel()

    def getLabel(self):
        """
        returns the major form label or None
        """
        ordered_forms = sorted(self['edges']['label'])
        if len(ordered_forms) > 0:
            return ordered_forms[0]
        else:
            return None

    def getPostag(self):
        """
        returns the major form POS tag or None
        """
        ordered_forms = sorted(self['edges']['postag'])
        if len(ordered_forms) > 0:
            return ordered_forms[0]
        else:
            return None
