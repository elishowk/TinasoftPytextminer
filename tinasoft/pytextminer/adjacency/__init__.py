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

#from multiprocessing import Process, Queue, Manager
import tinasoft

from datetime import date
from decimal import Decimal
import itertools

from numpy import *

import logging
_logger = logging.getLogger('TinaAppLogger')

class Matrix():
    """
    Matrix class to build adjacency matrix using numpy
    Manages external IDs as keys and constructs its internal auto-incremented reversed index
    """
    def __init__(self, size, type=int32):
        self.reverse = {}
        self.lastindex = -1
        self.array = zeros((size,size),dtype=type)

    def _getindex(self, key):
        if key in self.reverse:
            return self.reverse[key]
        else:
            self.lastindex += 1
            self.reverse[key] = self.lastindex
            return self.lastindex

    def get( self, key1, key2=None ):
        """
        Getter returning rows or cell from the array
        """
        if key2 is None:
            return self.array[ self._getindex(key1), : ]
        else:
            return self.array[ self._getindex(key1), self._getindex(key2) ]

    def set( self, key1, key2, value=1, overwrite=False ):
        """Setter using sums to increment cooc values"""
        index1 = self._getindex(key1)
        index2 = self._getindex(key2)
        if not overwrite:
            self.array[ index1, index2 ] += value
        else:
            self.array[ index1, index2 ] = value

class Adjacency(object):
    """
    A simple cooccurrences matrix processor
    Uses directly the data from storage
    """
    def __init__(self, config, storage, corpusid, opts):
        """
        Attach storage, a corpus objects and init the Matrix
        """
        self.config = config
        self.storage = storage
        self.corpusid = corpusid
        # loads the corpus object
        self.corpus = self.storage.loadCorpus( self.corpusid )
        if self.corpus is None:
            raise Warning('Corpus not found')
        self._loadProximity(opts)

    def _loadProximity(self, opts):
        # set the proximity method
        try:
            if 'proximity' in opts:
                # string eval to method
                self.proximity = getattr(self, opts['proximity'])
        except Exception, exc:
            self.proximity = None

    def walkDocuments(self):
        """
        walks through a list of documents into a corpus
        to process cooccurences
        """
        totaldocs = len(self.corpus['edges']['Document'].keys())
        doccount = 0
        for doc_id in self.corpus['edges']['Document'].iterkeys():
            obj = self.storage.loadDocument( doc_id )
            if obj is not None:
                self.proximity( obj )
                doccount += 1
                self.notify( doccount, totaldocs )
            else:
                _logger.warning("document %s not found in database"%doc_id)
        return self.matrix

    def notify(self, doccount, totaldocs):
        if doccount % 50 == 0:
            _logger.debug(
                'processed proximities for %d of %d documents in period %s'\
                    %(doccount,totaldocs,self.corpusid)
            )

class NgramAdjacency(Adjacency):
    """
    A simple cooccurrences matrix processor
    Uses directly the data from storage
    """
    def __init__(self, config, storage, corpusid, opts):
        Adjacency.__init__(self, config, storage, corpusid, opts)
        self.config['datamining']['NGramGraph'].update(opts)
        self.matrix = Matrix( len( self.corpus['edges']['NGram'].keys() ), type=float )
        if self.proximity is None:
            # loads default
            _logger.debug("loading default proximity %s"%self.config['datamining']['NGramGraph']['proximity'])
            self._loadProximity( self.config['datamining']['NGramGraph'] )

    def cooccurrences( self, document ):
        valid_keys = set(document['edges']['NGram'].keys()) & set(self.corpus['edges']['NGram'].keys())
        map = dict.fromkeys(valid_keys, 1)
        # map is a unity slice of the matrix
        for (ngi, ngj) in itertools.combinations(map.keys(), 2):
            self.matrix.set( ngi, ngj, 1 )
            self.matrix.set( ngj, ngi, 1 )

    def pseudoInclusion( self, document ):
        self.cooccurrences(document)
        valid_keys = set(document['edges']['NGram'].keys()) & set(self.corpus['edges']['NGram'].keys())
        alpha = self.config['datamining']['NGramGraph']['alpha']
        for (ng1, ng2) in itertools.permutations(valid_keys, 2):
            cooc = self.matrix.get(ng1, ng2)
            occ1 = self.corpus['edges']['NGram'][ng1]
            occ2 = self.corpus['edges']['NGram'][ng2]
            prox = (( cooc / occ1 )**alpha) * (( cooc / occ2 )**(1/alpha))
            self.matrix.set( ng1, ng2, prox, True )


class DocAdjacency(Adjacency):
    """
    Stores a index based on the size of the intersection of ngrams
    between documents
    """
    def __init__(self, config, storage, corpusid, opts ):
        Adjacency.__init__(self, config, storage, corpusid, opts )
        self.config['datamining']['DocumentGraph'].update(opts)
        self.matrix = Matrix( len( self.corpus['edges']['Document'].keys() ), type=float )
        if self.proximity is None:
            # loads default
            self._loadProximity( self.config['datamining']['DocumentGraph'] )
        # prepares some cache vars
        self.corpus_ngrams = set( self.corpus['edges']['NGram'] )
        self.documents = {}

    def sharedNgrams( self, document ):
        """
        intersection of doc's ngrams every other document
        then return length of the intersection with doc2 ngrams
        """
        ngrams = set(document['edges']['NGram'].keys()) & self.corpus_ngrams
        self.documents[document['id']] = ngrams
        for doc1, doc2 in itertools.combinations( self.documents.keys(), 2 ):
            prox = len( self.documents[doc1] & self.documents[doc2] )
            self.matrix.set( doc1, doc1, prox )
            self.matrix.set( doc2, doc1, prox )


    def logJaccard( self, document ):
        """
        Jaccard-like distance
        """
        ngrams = set(document['edges']['NGram'].keys())
        self.documents[document['id']] = ngrams
        for doc1, doc2 in itertools.combinations( self.documents.keys(), 2 ):
            ngramsintersection = self.documents[doc1] & self.documents[doc2] & self.corpus_ngrams
            ngramsunion = self.documents[doc1] | self.documents[doc2] | self.corpus_ngrams
            if len(ngramsunion) > 0:
                weight = sum(
                    [ 1/(math.log( 1 + self.corpus['edges']['NGram'][ngi] )) for ngi in ngramsintersection ],
                    0
                ) / sum(
                    [ 1/(math.log( 1 + self.corpus['edges']['NGram'][ngj] )) for ngj in ngramsunion ],
                    0
                )
            self.matrix.set(doc1, doc2, weight)

