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

import logging
_logger = logging.getLogger('TinaAppLogger')

class NGram(PyTextMiner):
    """NGram class"""
    def __init__(self, tokenlist, id=None, label=None, edges=None, postag=None, **metas):
        """
        initiate the object
        normalize must be local value for pickling reasons
        """
        # normlist is the normalized list of tokens
        normalized_tokens = [NGram.normalize(word) for word in tokenlist]
        # prepares postag
        if postag is not None:
            metas["postag"] = postag
        else:
            metas["postag"] = ["?"]
        # default emtpy edges
        if edges is None:
            edges = { 'label': {}, 'postag' : {} }
        PyTextMiner.__init__(self, normalized_tokens, id, label, edges, **metas)
        # updates majors forms before returning instance
        self.addForm(normalized_tokens, metas["postag"], 1)

    def addEdge(self, type, key, value):
        """
        The NGram object accepts only one edge write to a Document object
        All the ohter edges are multiples
        """
        if type in ["Document"]:
            return self._addUniqueEdge( type, key, value )
        elif type in ["NGram"]:
            return self._overwriteEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )

    @staticmethod
    def normalize(word):
        """
        Default tokens normalizing
        """
        return word.lower()

    @staticmethod
    def getNormId(tokens):
        """
        Utility returning a normalized NGram ID given a tokenlist
        For external use
        """
        # normalized_tokens list to produce an unique id
        normalized_tokens = [NGram.normalize(word) for word in tokens]
        return PyTextMiner.getId(normalized_tokens)

    def updateMajorForm(self):
        """
        updates major form of a nlemma
        """
        # updates major form label attr
        self.postag = self.getPostag()
        self.label = self.getLabel()
        self.content = self.label.split(" ")

    def getLabel(self):
        """
        returns the major form label or None
        """
        ordered_forms = sorted(self['edges']['label'])
        if len(ordered_forms) > 0:
            return ordered_forms[-1]
        else:
            return self.label

    def getPostag(self):
        """
        returns the major form POS tag or None
        """
        ordered_forms = sorted(self['edges']['postag'])
        if len(ordered_forms) > 0:
            return ordered_forms[-1]
        else:
            return self.postag

    def addForm(self, form_tokens, form_postag, form_occs=1 ):
        # updates edges
        form_label = PyTextMiner.form_label( form_tokens )
        form_postag_label = PyTextMiner.form_label( form_postag )
        self.addEdge('label', form_label, form_occs)
        self.addEdge('postag', form_postag_label, form_occs)
        #self.updateMajorForm()

