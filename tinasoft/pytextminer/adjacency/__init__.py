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

import itertools
from numpy import *
import math

import logging
_logger = logging.getLogger('TinaAppLogger')

def ngram_adj_task( config, storage, period, ngramgraphconfig, matrix_index, whitelist ):
    try:
        adj = NgramAdjacency( config, storage, period, ngramgraphconfig, matrix_index, whitelist )
        submatrix_gen = adj.generator()
        while 1:
            yield submatrix_gen.next()
    except Warning, warn:
        _logger.warning( str(warn) )
        return
    except StopIteration, si:
        _logger.debug("NgramAdjancency on period %s is finished"%period)

def document_adj_task( config, storage, period, documentgraphconfig, matrix_index, whitelist ):
    try:
        adj = DocAdjacency( config, storage, period, documentgraphconfig, matrix_index, whitelist )
        submatrix_gen = adj.generator()
        while 1:
            yield submatrix_gen.next()
    except Warning, warn:
        _logger.warning( str(warn) )
        return
    except StopIteration, si:
        _logger.debug("DocAdjacency on period %s is finished"%period)

class Matrix():
    """
    Matrix class to build adjacency matrix using numpy
    Manages external IDs with an auto-incremented internal index
    """
    def __init__(self, index, valuesize=float64):
        """
        Creates the numpy array and the index dictionary
        """
        self.reverseindex = {}
        for i, key in enumerate(index):
            self.reverseindex[key] = i
        size = len(index)
        try:
            self.array = zeros((size, size), dtype=valuesize)
        except Exception, exc:
            _logger.error(exc)
            raise Exception(str(exc))


    def _getindex(self, key):
        """
        returns the internal auto-incremented index from an external ID
        """
        return self.reverseindex[key]

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
        # then writes the value
        if overwrite is False:
            self.array[ index1, index2 ] += value
        else:
            self.array[ index1, index2 ] = value

class SymmetricMatrix(Matrix):
    """
    Specialized semi numpy 2D array container
    Gets and Set only upper part of the matrix
    SymmetricMatrix.get(key1,key2) === SymmetricMatrix.get(key2,key1)
    DO NOT increment values twice !!!
    """
    def __init__(self, *args, **kwargs):
        Matrix.__init__(self, *args, **kwargs)

    def get( self, key1, key2=None ):
        """
        Getter returning rows or cell from the matrix
        """
        if key2 is None:
            return self.array[ self._getindex(key1), : ]
        else:
            indices = [self._getindex(key1), self._getindex(key2)]
            indices.sort()
            return self.array[ indices[0], indices[1] ]

    def set( self, key1, key2, value=1, overwrite=False ):
        """
        Increments cooc array using boolean multiplication
        Sort keys and avoid duplicates values.
        """
        indices = [self._getindex(key1), self._getindex(key2)]
        indices.sort()
        if overwrite is False:
            self.array[ indices[0], indices[1] ] += value
        else:
            self.array[ indices[0], indices[1] ] = value

    def extract_matrix(self, minCooc=1):
        """
        yields all values of the upper part of the matrix
        associating ngrams with theirs tinasoft's id
        """
        countcooc = 0
        for i in range(self.size):
            ngi = self.id_index[i]
            row = {}
            coocline = self.matrix[i,:]
            for j in range(self.size - i):
                cooc = coocline[i+j]
                if cooc >= minCooc:
                    countcooc += 1
                    ngj = self.id_index[j]
                    row[ngj] = cooc
            if len(row.keys()) > 0:
                yield (ngi, row)
        _logger.debug("found %d non-zero cooccurrences values"%countcooc)

class MatrixReducer(Matrix):
    """
    Generic matrix additioner
    """
    def __init__(self, *args, **kwargs):
        Matrix.__init__(self, *args, **kwargs)

    def add(self, submatrix):
        """"callback adding a submatrix into the self-contained matrix"""
        if submatrix is not None:
            for external in submatrix.reverseindex.keys():
                self.set( external, external, value=submatrix.get( external, external ), overwrite=False )
            for (external1, external2) in itertools.permutations( submatrix.reverseindex.keys(), 2):
                addvalue = submatrix.get( external1, external2 )
                if addvalue != 0:
                    self.set(external1, external2, value=addvalue, overwrite=False)

class Adjacency(object):
    """
    Base class
    A simple adjacency matrix processor
    """
    def __init__( self, config, storage, corpusid, opts, name, index, whitelist ):
        """
        Attach storage, a corpus objects and init the Matrix
        """
        self.config = config
        self.storage = storage
        self.corpusid = corpusid
        # loads the corpus object
        self.corpus = self.storage.loadCorpus( self.corpusid )
        # TODO reduce corpus ngrams to the index contents
        if self.corpus is None:
            raise Warning('Corpus not found')
        self.name = name
        self._loadOptions(opts)
        self.index = index
        self.whitelist = set( whitelist['edges']['NGram'].keys() )

    def _loadDefaults(self):
        """
        loads default from self.config
        """
        for attr, value in self.config['datamining'][self.name].iteritems():
            setattr(self,attr,value)
        try:
            if 'proximity' in self.config['datamining'][self.name]:
                # string eval to method
                self.proximity = getattr(self, self.config['datamining'][self.name]['proximity'])
                #del self.config['datamining'][self.name]['proximity']
        except Exception, exc:
            _logger.error("impossible to load %s"%self.config['datamining'][self.name]['proximity'])

    def _loadOptions(self, opts):
        """
        First laods default options
        Then overwrites with eventual user options
        """
        self._loadDefaults()
        # override default config
        for attr, value in opts.iteritems():
            setattr(self,attr,value)
        # override the proximity method
        try:
            if 'proximity' in opts:
                # string eval to method
                _logger.debug("setting up proximity from paramater to %s"%opts['proximity'])
                self.proximity = getattr(self, opts['proximity'])
                #del opts['proximity']
        except Exception, exc:
            _logger.error("impossible to load %s"%opts['proximity'])
            self.proximity = None


    def proximity(self, document):
        raise Exception("proximity method not defined")
        return 0

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
                yield obj
                doccount += 1
                self.notify( doccount, totaldocs )
            else:
                _logger.warning("document %s not found in database"%doc_id)

    def notify(self, doccount, totaldocs):
        if doccount % 50 == 0:
            _logger.debug(
                'done %s proximities for %d of %d documents in period %s'\
                    %(self.name, doccount,totaldocs,self.corpusid)
            )

class NgramAdjacency(Adjacency):
    """
    A NGram graph adjacency processor
    """
    def __init__(self, config, storage, corpusid, opts, index, whitelist ):
        Adjacency.__init__(self, config, storage, corpusid, opts, 'NGramGraph', index, whitelist )

    def cooccurrences( self, document ):
        """
        simple document cooccurrences matrix calculator
        """
        submatrix = SymmetricMatrix( document['edges']['NGram'].keys(), valuesize=float32 )
        valid_keys = set(document['edges']['NGram'].keys()) & self.whitelist
        # map is a unity slice of the matrix
        for (ngi, ngj) in itertools.combinations(valid_keys, 2):
            submatrix.set( ngi, ngj, 1 )
            submatrix.set( ngj, ngi, 1 )
        for ngi in valid_keys:
            # diagonal set to node occurrence
            submatrix.set( ngi, ngi, value=self.corpus['edges']['NGram'][ngi], overwrite=True )
        return submatrix

    def pseudoInclusion( self, document ):
        """
        uses cooccurrences matrix to process pseudo-inclusion proximities
        """
        submatrix = self.cooccurrences( document )
        valid_keys = set(document['edges']['NGram'].keys()) & self.whitelist
        alpha = self.config['datamining']['NGramGraph']['alpha']
        for (ng1, ng2) in itertools.permutations(valid_keys, 2):
            if ng1 != ng2:
                cooc = submatrix.get(ng1, ng2)
                occ1 = self.corpus['edges']['NGram'][ng1]
                occ2 = self.corpus['edges']['NGram'][ng2]
                prox = (( cooc / occ1 )**alpha) * (( cooc / occ2 )**(1/alpha))
                submatrix.set( ng1, ng2, value=prox, overwrite=True )
            # else : let the initial occurence value into the DB
        return submatrix

    def generator(self):
        """
        walks through a list of documents into a corpus
        to process proximities
        """
        generator = self.walkDocuments()
        try:
            while 1:
                document = generator.next()
                yield self.proximity( generator.next() )
        except StopIteration, si:
            return

class DocAdjacency(Adjacency):
    """
    A Document graph adjacency processor
    """
    def __init__(self, config, storage, corpusid, opts, index, whitelist ):
        Adjacency.__init__(self, config, storage, corpusid, opts, 'DocumentGraph', index, whitelist )

    def sharedNgrams( self, document ):
        """
        intersection of doc's ngrams every other document
        then return length of the intersection with doc2 ngrams
        """
        submatrix = SymmetricMatrix( self.corpus['edges']['Document'].keys(), valuesize=int32 )
        ngrams = set(document['edges']['NGram'].keys()) & self.whitelist
        documentobj = self.storage.loadDocument(docid)

        for docid in self.corpus['edges']['Document'].iterkeys():
            # trick to calculate doc proximity only on time
            if submatrix.get(document['id'], docid) != 0 or submatrix.get( docid, document['id'] ) != 0:
                _logger.debug("skipping doc proximity")
                continue
            if docid != document['id']:
                documentobj = self.storage.loadDocument(docid)
                if documentobj is None: continue
                prox = len( ngrams & documentobj['edges']['NGram'].keys() )
                submatrix.set( document['id'], docid, value=prox )

            else:
                # diagonal set to node weight
                submatrix.set( docid, docid, value=self.corpus['edges']['Document'][docid], overwrite=True )
        return submatrix


    def logJaccard( self, document ):
        """
        Jaccard-like distance
        """
        submatrix = SymmetricMatrix( self.corpus['edges']['Document'].keys(), valuesize=float32 )
        doc1ngrams = set(document['edges']['NGram'].keys()) & self.whitelist
        #TODO reduce calculation to the symmetric matrix !!!
        for docid in self.index:
            # trick to calculate doc proximity only on time
            if submatrix.get(document['id'], docid) != 0 or submatrix.get( docid, document['id'] ) != 0:
                _logger.debug("skipping doc proximity")
                continue
            if docid == document['id']:
                # diagonal set to node weight
                submatrix.set( docid, docid, value=self.corpus['edges']['Document'][docid], overwrite=True )
                #_logger.debug(  "setting doc matrix diagonal to %d"%submatrix.get(docid, docid) )
            else:
                documentobj = self.storage.loadDocument(docid)
                if documentobj is None: continue

                doc2ngrams = set(documentobj['edges']['NGram'].keys()) & self.whitelist
                ngramsintersection = doc1ngrams & doc2ngrams
                ngramsunion = doc1ngrams | doc2ngrams
                weight = 0
                numerator = 0
                for ngi in ngramsintersection:
                    numerator += 1/(math.log( 1 + self.corpus['edges']['NGram'][ngi] ))
                denominator = 0
                for ngi in ngramsunion:
                    denominator += 1/(math.log( 1 + self.corpus['edges']['NGram'][ngi] ))
                if denominator > 0:
                    weight = numerator / denominator
                submatrix.set(document['id'], docid, weight)

        return submatrix

    def generator(self):
        """
        overwrites Adjacency.walkDocuments() to return processed proximities
        """
        generator = self.walkDocuments()
        try:
            while 1:
                document = generator.next()
                yield self.proximity( generator.next() )
        except StopIteration, si:
            return
