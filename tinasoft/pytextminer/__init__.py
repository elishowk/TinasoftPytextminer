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

__all__ = ["filtering","corpora", "corpus", "document", "ngram", "tokenizer", "tagger", "cooccurrences", "stopwords","extractor"]

from uuid import uuid4

import logging
_logger = logging.getLogger('TinaAppLogger')

class PyTextMiner():

    """
        PyTextMiner class
        parent of classes analyzed by this module
        common to all analysed objects in this software :
        ngram, document, etc
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
        for attr, value in metas.iteritems():
            setattr(self,attr,value)

    @staticmethod
    def getId(content):
        if content is None:
            return uuid4().hex
        if type(content).__name__ == 'list':
            # for NGrams
            return str(abs( hash( " ".join(content) ) ))
        elif type(content).__name__ == 'str' or type(content).__name__ == 'unicode':
            # for all other string content
            return str(abs( hash( content ) ))
        else:
            raise ValueError
            return None

    def _addUniqueEdge( self, type, key, value ):
        if type not in self['edges']:
            self['edges'][type]={}
            #return 0
        if key in self['edges'][type]:
            return False
        else:
            self['edges'][type][key] = value
            return True

    def _addEdge(self, type, key, value):
        #_logger.debug(key)
        if type not in self['edges']:
            self['edges'][type]={}
        if key in self['edges'][type]:
            self['edges'][type][key] += value
        else:
            self['edges'][type][key] = value
        return True

    def addEdge(self, type, key, value):
        return self._addEdge( type, key, value )


    def normalize(self, tokenlst):
        return [token.lower() for token in tokenlst]

    def __getitem__(self, key):
        return getattr( self, key, None )

    def __setitem__(self, key, value):
        setattr( self, key, value )

    def __delitem__(self, key):
        delattr(self, key)

    def __contains__(self, key):
        try:
            getattr(self, key, None)
            return True
        except AttributeError, a:
            return False
