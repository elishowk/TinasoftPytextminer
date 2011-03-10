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

from re import sub
from hashlib import sha256
from uuid import uuid4
import codecs

import logging
_logger = logging.getLogger('TinaAppLogger')

class PyTextMiner(object):

    """
    PyTextMiner class
    is a the parent class of all the graph nodes classes
    """

    def __init__(self, content, id=None, label=None, edges=None, **metas):
        """
        Default Tinasoft's graph node object initialization
        """
        self.content = content
        if id is None:
            self.id = PyTextMiner.getId( content )
        else:
            self.id = sub(r"[^a-zA-Z0-9_\-]","",id)
        if label is None:
            label = PyTextMiner.form_label( content )
        self.label = label
        self._loadEdges( edges )
        self._loadOptions( metas )

    def _loadOptions(self, metas):
        """
        Transforms metas to obj attributes
        """
        for attr, value in metas.iteritems():
            setattr(self,attr,value)

    def _loadEdges(self, edges):
        """
        used only by __init__
        """
        defaultedges = { 'Document' : {}, 'NGram' : {}, 'Corpus': {}, 'Corpora': {}, 'Whitelist': {}, 'Cluster': {} }
        if edges is not None:
            defaultedges.update(edges)
        self.edges = defaultedges

    def _cleanEdges(self, *args, **kwargs):
        for targettype in self['edges'].keys():
            for targetid in self['edges'][targettype].keys():
                if self['edges'][targettype][targetid] <= 0:
                    del self['edges'][targettype][targetid]

    def updateEdges(self, updateedges, force=False):
        """
        merges an object's edges with the candidate edges dictionary
        FORCING to sum values
        """
        for targettype in updateedges.iterkeys():
            for targetid, weight in updateedges[targettype].iteritems():
                if force is True:
                    self._addEdge( targettype, targetid, weight )
                else:
                    self.addEdge( targettype, targetid, weight )
#                self._addEdge( targettype, targetid, weight )

    def updateObject(self, obj, force=False):
        """ overwrites all attributes then updates edges """
        for key in obj.__dict__.keys():
            if key == "edges": continue
            self[key]=obj[key]
        self.updateEdges( obj["edges"], force )
        
    @staticmethod
    def form_label(tokens):
        """
        common method forming clean labels from unicode token list
        """
        label = " ".join(tokens)
        if not isinstance(label, unicode ):
            return unicode(label , "utf_8", errors='ignore')
        else:
            return label

    @staticmethod
    def getId(content):
        """
        Common staticmethod constructing an ID str for all PyTextMiner objects
        @content must be a list of str
        """
        if type(content) == list:
            convert = PyTextMiner.form_label(content)
            return sha256( convert.encode( 'ascii', 'replace' ) ).hexdigest()
        else:
            return uuid4().hex

    def getLabel(self):
        """
        returns the label
        """
        return self.label

    def _addUniqueEdge( self, type, key, value ):
        """
        low level method writing ONLY ONCE a weighted edge to a PyTextMiner object
        """
        if type not in self['edges']:
            self['edges'][type] = {}
        if key in self['edges'][type]:
            return False
        else:
            self['edges'][type][key] = value
            return True

    def _overwriteEdge(self, type, key, value):
        """
        low level method overwriting a weighted edge to a PyTextMiner object
        """
        if type not in self['edges']:
            self['edges'][type]={}
        self['edges'][type][key] = value

    def _addEdge(self, type, key, value):
        """
        low level method incrementing a weighted edge to a PyTextMiner object
        """
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
        return getattr( self, key, None )

    def __setitem__(self, key, value):
        """
        compatibility with the dict class
        """
        setattr( self, key, value )

    def __delitem__(self, key):
        """
        compatibility with the dict class
        """
        try:
            delattr(self, key)
        except AttributeError:
            return

    def __contains__(self, key):
        """
        compatibility with the dict class
        """
        try:
            getattr(self, key, None)
            return True
        except AttributeError:
            return False
