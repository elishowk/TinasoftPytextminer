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


__author__="Julian Bilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:30:11 PM$"

__all__ = [
    "corpora", "corpus", "document", "ngram","whitelist"
    "filtering","tokenizer", "tagger", "cooccurrences", "stopwords","extractor","stemmer"
]

from uuid import uuid4

import logging
_logger = logging.getLogger('TinaAppLogger')

class PyTextMiner():

    """
        PyTextMiner class
        is a the parent class of the graph nodes classes
    """

    def __init__(self, content, id=None, label=None, edges=None, **metas):
        self.content = content
        if id is None:
            self.id = PyTextMiner.getId( content )
        else:
            self.id = id
        self.label = label
        if not edges:
            edges = {}
        self.edges = edges
        self.loadOptions( metas )

    def loadOptions(self, metas):
        """
        Transforms metas to obj attributes
        """
        for attr, value in metas.iteritems():
            setattr(self,attr,value)

    @staticmethod
    def getId(content):
        """
        Common staticmethod constructing an ID str for all PyTextMiner objects
        """
        if content is None:
            return uuid4().hex
        if type(content) == list:
            # for NGrams-like objects : content==list
            return str(abs( hash( " ".join(content) ) ))
        elif type(content) == str or type(content) == unicode:
            # for all other string content
            return str(abs( hash( content ) ))
        else:
            raise ValueError
            return None

    def getLabel(self):
        """
        returns the label
        """
        return self.label

    @staticmethod
    def updateEdges(canditate, update, types):
        """updates an existent object's edges with the candidate object's edges"""
        for targets in types:
            for targetsId, targetWeight in canditate['edges'][targets].iteritems():
                res = update.addEdge( targets, targetsId, targetWeight )
        return update

    def _addUniqueEdge( self, type, key, value ):
        """
        low level method adding ONLY ONCE a weighted edge to a PyTextMiner object
        """
        if not isinstance(key,str):
            key=str(key)
        if type not in self['edges']:
            self['edges'][type]={}
        if key in self['edges'][type]:
            return False
        else:
            self['edges'][type][key] = value
            return True

    def _addEdge(self, type, key, value):
        """
        low level method adding or incrementing a weighted edge to a PyTextMiner object
        """
        if not isinstance(key,str):
            key=str(key)
        if type not in self['edges']:
            self['edges'][type]={}
        if key in self['edges'][type]:
            self['edges'][type][key] += value
        else:
            self['edges'][type][key] = value
        return True

    def __getitem__(self, key):
        """
        compatibility with the dict class
        """
        if not isinstance(key,str):
            key=str(key)
        return getattr( self, key, None )

    def __setitem__(self, key, value):
        """
        compatibility with the dict class
        """
        if not isinstance(key,str):
            key=str(key)
        setattr( self, key, value )

    def __delitem__(self, key):
        """
        compatibility with the dict class
        """
        if not isinstance(key,str):
            key=str(key)
        delattr(self, key)

    def __contains__(self, key):
        """
        compatibility with the dict class
        """
        try:
            if not isinstance(key,str):
                key=str(key)
            getattr(self, key, None)
            return True
        except AttributeError, a:
            return False
