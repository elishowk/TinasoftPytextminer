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
from tinasoft.pytextminer import corpus, filtering
from datetime import date

from numpy import *

import logging
_logger = logging.getLogger('TinaAppLogger')

class CoocMatrix():
    """
    Matrix class counting cooccurrences
    Manages external IDs as keys and constructs its internal auto-tincremented reversed index
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
            return self.reverse[key]

    def get( self, key1, key2=None ):
        """
        Getter returning rows or cell from the array
        """
        if key2 is None:
            return self.array[ self._getindex(key1), : ]
        else:
            return self.array[ self._getindex(key1), self._getindex(key2) ]

    def set( self, key1, key2, value=1 ):
        """Setter using sums to increment cooc values"""
        index1 = self._getindex(key1)
        index2 = self._getindex(key2)
        self.array[ index1, index2 ] += value

class Simple():
    """
    A simple cooccurrences matrix processor
    Uses directly the data from storage
    """
    def __init__(self, storage, corpusid):
        self.storage = storage
        self.corpusid = corpusid
        self.corpus = self.storage.loadCorpus( self.corpusid )
        if self.corpus is None:
            raise Warning('Corpus not found')
        self.matrix = CoocMatrix( len( self.corpus['edges']['NGram'].keys() ) )

    def walkCorpus(self):
        """processes a list of documents into a corpus"""
        totaldocs = len(self.corpus['edges']['Document'].keys())
        doccount = 0
        for doc_id in self.corpus['edges']['Document']:
            obj = self.storage.loadDocument( doc_id )
            if obj is not None:
                mapgenerator = self.mapper(obj)
                try:
                    # documents loop
                    while mapgenerator:
                        term_map = mapgenerator.next()
                        self.reducer( term_map )
                except StopIteration, si: pass
            doccount += 1
            if doccount % 50 == 0:
                _logger.debug(
                    'processed coocs for %d of %d documents in period %s'%(doccount,totaldocs,self.corpusid)
                )

    def mapper(self, doc):
        """
        generates a row for each ngram in the doc,
        cooccurrences to 1 to every other ngram in the doc
        """
        map = dict.fromkeys(doc['edges']['NGram'].keys(), 1)
        # map is a unity slice of the matrix
        for ng in doc['edges']['NGram'].keys():
            yield [ng,map]

    def reducer(self, term_map):
        """updates the cooc matrix"""
        ng1 = term_map[0]
        map = term_map[1]
        for ngi in map.iterkeys():
            self.matrix.set( ng1, ngi )


    def writeMatrix(self, overwrite=True):
        """
        writes in the db rows of the matrix
        'Cooc::corpus::ngramid' => '{ 'ngx' : y, 'ngy': z }'
        """

        if self.corpusid is not None:
            key = self.corpusid+'::'
        else:
            return
        countcooc = 0
        for ngi in self.matrix.reverse.iterkeys():
            row = {}
            for ngj in self.matrix.reverse.iterkeys():
                cooc = self.matrix.get( ngi, ngj )
                if cooc > 0:
                    countcooc += 1
                    row[ngj] = cooc
            if len( row.keys() ) > 0:
                self.storage.updateCooc( key+ngi, row, overwrite )
        _logger.debug( 'will store %d non-zero cooc values'%countcooc )
        self.storage.flushCoocQueue()
        _logger.debug( 'finished storing %d non-zero cooc values'%countcooc )

    def readMatrix( self ):
        try:
            generator = self.storage.selectCorpusCooc( self.corpusid )
            while 1:
                id,row = generator.next()
                self.reducer( [id, row] )
        except StopIteration, si:
            return self.matrix



class Filtered(Simple):
    """
    TO TEST
    """
    def __init__(self, storage, whitelist=None, corpusid=None, filter=None):
        self.storage = storage
        self.filter = filter
        self.corpusid = corpusid
        self.corpus = self.storage.loadCorpus( self.corpusid )
        if self.corpus is None:
            raise Warning('Corpus not found')
        self.whitelist = whitelist
        if self.whitelist is not None:
            self.matrix = CoocMatrix( len( self.whitelist['edges']['NGram'].keys() ) )
        else:
            self.matrix = CoocMatrix( len( self.corpus['edges']['NGram'].keys() ) )

    def filterNGrams(self, ngrams):
        """
        Construct the filtered & white-listed
        slice of the matrix
        """
        map = {}
        for ng in ngrams:
            obj = self.storage.loadNGram(ng)
            if obj is None:
                continue
            if self.whitelist.test(obj) is False:
                continue
            if filtering.apply_filters( obj, self.filter ) is False:
                continue
            map[ng] = 1
        return map

    def mapper(self, doc):
        """
        generates a row for each ngram in the doc,
        cooccurrences to 1 to every other ngram in the doc
        """
        ngrams = doc['edges']['NGram'].keys()
        map = self.filterNGrams(ngrams)
        # map is a unity slice of the matrix
        for ng in map.keys():
            yield [ng,map]

