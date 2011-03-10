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
        All other edges are multiples
        """
#        if type in ["Document"]:
#            return self._addUniqueEdge( type, key, value )
        if type in ["NGram", "postag"]:
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
        self.label = self.getLabel()
        self.content = self.label.split(" ")
        self.postag = self['edges']['postag'][self.label]

    def getLabel(self):
        """
        returns the major form label or None
        """
        ordered_forms = sorted(self['edges']['label'])
        if len(ordered_forms) > 0:
            return ordered_forms[-1]
        else:
            return self.label

    def addForm(self, form_tokens, form_postag=None, form_occs=1 ):
        """
        increments form edges
        """
        if form_postag is None:
            form_postag = ["?"]
        form_label = PyTextMiner.form_label( form_tokens )
        self.addEdge('label', form_label, form_occs)
        self.addEdge('postag', form_label, form_postag)

    def newToGraph(self, document, corpus):
        """
        Used to index a new NGram within a graph, given @document and a @corpus
        """
        corpusId = corpus['id']
        ngid = self['id']
        docOccs = self['occs']
        del self['occs']
        ### write if not already exists Doc-NGram edges
        self.addEdge( 'Document', document['id'], docOccs )
        document.addEdge( 'NGram', ngid, docOccs )
        ### updates Corpus-NGrams edges
        self.addEdge( 'Corpus', corpusId, 1 )
        corpus.addEdge( 'NGram', ngid, 1 )
        return document, corpus

    def deleteForm(self, form, is_keyword=False):
        """
        removes a form
        """
        if form in self['edges']['label']:
            del self['edges']['label'][form]
        if form in self['edges']['postag']:
            del self['edges']['postag'][form]
        if is_keyword and 'keyword' in self['edges'] and form in self['edges']['keyword']:
            del self['edges']['keyword'][form]