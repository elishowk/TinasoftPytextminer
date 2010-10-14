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

def ngram_submatrix_task( config, storage, period, ngramgraphconfig, matrix_index, whitelist ):
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

def document_submatrix_task( config, storage, period, documentgraphconfig, matrix_index, whitelist ):
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

class Matrix(object):
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
    OBSOLETE
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



class MatrixReducer(Matrix):
    """
    Generic matrix additioner
    """
    def __init__(self, index, valuesize=float32):
        Matrix.__init__(self, index, valuesize=valuesize)

    def add(self, submatrix):
        """"callback adding a submatrix into the self-contained matrix"""
        if submatrix is not None:
            for (external1, external2) in itertools.permutations( submatrix.reverseindex.keys(), 2):
                self.set(external1, external2, value=submatrix.get( external1, external2 ), overwrite=False)

    def extract_semiupper_matrix(self, config):
        """
        yields all values of the upper part of the matrix
        associating ngrams with theirs tinasoft's id
        """
        count = 0
        id_index = {}
        for key, idx in self.reverseindex.iteritems():
            id_index[idx] = key
        for i in range(self.array.shape[0]):
            # node filter
            if self.array[i,i] < float(config['nodethreshold'][0]) or self.array[i,i] > float(config['nodethreshold'][1]): continue
            nodei = id_index[i]
            row = {}
            for j in range(self.array.shape[0] - i):
                if self.array[i+j,i+j] < float(config['nodethreshold'][0]) or self.array[i+j,i+j] > float(config['nodethreshold'][1]): continue

                prox = self.array[i,i+j]
                if prox >= float(config['edgethreshold'][0]):
                    if max is None:
                        count += 1
                        nodej = id_index[j]
                        row[nodej] = prox
                    elif prox <= float(config['edgethreshold'][1]):
                        count += 1
                        nodej = id_index[j]
                        row[nodej] = prox
            if len(row.keys()) > 0:
                yield (nodei, row)
        _logger.debug("found %d valid proximity values"%count)

    def extract_matrix(self, config):
        """
        yields all values of the matrix
        associating ngrams with theirs tinasoft's id
        """
        count = 0
        id_index = {}
        for key, idx in self.reverseindex.iteritems():
            id_index[idx] = key

        for i in range(self.array.shape[0]):
            nodei = id_index[i]
            # node filter
            if self.array[i,i] < float(config['nodethreshold'][0]) or self.array[i,i] > float(config['nodethreshold'][1]): continue
            row = {}
            for j in range(self.array.shape[0]):
                if i == j : continue
                # node filter
                if self.array[j,j] < float(config['nodethreshold'][0]) or self.array[j,j] > float(config['nodethreshold'][1]): continue
                prox = self.array[i,j]
                # edge filter
                if prox >= float(config['edgethreshold'][0]):
                    if max is None:
                        count += 1
                        nodej = id_index[j]
                        row[nodej] = prox
                    elif prox <= float(config['edgethreshold'][1]):
                        count += 1
                        nodej = id_index[j]
                        row[nodej] = prox
            if len(row.keys()) > 0:
                yield (nodei, row)
        _logger.debug("found %d valid proximity values"%count)

    def export(self, path, index):
        fh = open(path, 'w+')
        fh.write(",".join(index))
        savetxt( fh, self.array, "%.2f", ",")


class PseudoInclusionMatrix(MatrixReducer):
    """
    Matrix Reducer used to store cooccurrence matrix,
    then extract pseudo-inclusion on the fly
    """

    def extract_matrix( self, config ):
        alpha = config['alpha']
        coocmatrix = super(PseudoInclusionMatrix, self).extract_matrix( config )
        try:
            count = 0
            while 1:
                ni, coocrow = coocmatrix.next()
                pirow = {}
                occi = self.get(ni, ni)
                for nj, cooc in coocrow.iteritems():
                    occj = self.get(nj, nj)
                    pi = (( cooc / occi )**alpha) * (( cooc / occj )**(1/alpha))
                    if pi >= float(config['edgethreshold'][0]):
                        if max is None:
                            count += 1
                            pirow[nj] = pi
                        elif pi <= float(config['edgethreshold'][1]):
                            count += 1
                            pirow[nj] = pi
                if len(pirow.keys()) > 0:
                    yield ni, pirow
        except StopIteration:
            _logger.debug("found %d valid pseudo-inclusion values"%count)

class Adjacency(object):
    """
    Base class
    A simple adjacency matrix processor
    """
    def __init__( self, config, storage, corpusid, opts, name, index, whitelist ):
        """
        Attach storage, a corpus objects and init the Matrix
        """
        self.storage = storage
        self.corpusid = corpusid
        # loads the corpus object
        self.corpus = self.storage.loadCorpus( self.corpusid )
        # TODO reduce corpus ngrams to the index contents
        if self.corpus is None:
            raise Warning('Corpus not found')
            return
        self.name = name
        self._loadOptions(opts, config)
        # sets intersection of ngrams from the whitelist and the corpus
        self.periodngrams = set(whitelist['edges']['NGram'].keys()) & set(self.corpus['edges']['NGram'].keys())

    def _loadDefaults(self, config):
        """
        loads default from config
        """
        for attr, value in config['datamining'][self.name].iteritems():
            setattr(self,attr,value)
        try:
            if 'proximity' in config['datamining'][self.name]:
                # string eval to method
                self.proximity = getattr(self, config['datamining'][self.name]['proximity'])

        except Exception, exc:
            _logger.error("impossible to load %s"%config['datamining'][self.name]['proximity'])

    def _loadOptions(self, opts, defaults):
        """
        First loads the default options
        Then overwrites with eventual user options
        """
        self._loadDefaults(defaults)
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
        walks through a sorted list of documents within a corpus
        """
        totaldocs = len(self.corpus['edges']['Document'].keys())
        doccount = 0
        sorted = self.corpus['edges']['Document'].keys()
        sorted.sort()
        for doc_id in sorted:
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
        valid_keys = set(document['edges']['NGram'].keys()) & self.periodngrams
        submatrix = Matrix( list(valid_keys), valuesize=float32 )
        # only processes a half of the symmetric matrix
        for (ngi, ngj) in itertools.combinations(valid_keys, 2):
            submatrix.set( ngi, ngj, 1 )
            submatrix.set( ngj, ngi, 1 )
        return submatrix

    def diagonal( self, matrix_reducer ):
        # global index
        for ng in self.periodngrams:
            matrix_reducer.set( ng, ng, value=self.corpus['edges']['NGram'][ng])

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
        # docngrams cache
        self.documentngrams = {}
        # pre-calculed maximum ngrams in documents
        self.maxngrams = 0
        for doc in self.corpus['edges']['Document'].keys():
            documentobj = self.storage.loadDocument(doc)
            self.documentngrams[doc] = set(documentobj['edges']['NGram'].keys()) & self.periodngrams
            if self.maxngrams < len(self.documentngrams[doc]):
                self.maxngrams = len(self.documentngrams[doc])

    def sharedNGrams( self, document ):
        """
        number of ngram shared by 2 documents dividedby the max over the period
        """
        submatrix = Matrix( self.corpus['edges']['Document'].keys(), valuesize=float32 )
        doc1ngrams = self.documentngrams[document['id']]
        for docid in self.documentngrams.keys():
            if docid != document['id']:
                doc2ngrams = self.documentngrams[docid]
                prox = len( doc1ngrams & doc2ngrams ) / self.maxngrams
                submatrix.set( document['id'], docid, value=prox, overwrite=True )
        return submatrix


    def logJaccard( self, document ):
        """
        Jaccard-like distance
        """
        submatrix = Matrix( self.corpus['edges']['Document'].keys(), valuesize=float32 )
        doc1ngrams = self.documentngrams[document['id']]

        for docid in self.documentngrams.keys():
            doc2ngrams = self.documentngrams[docid]
            ngramsintersection = doc1ngrams & doc2ngrams
            ngramsunion = (doc1ngrams | doc2ngrams)
            weight = 0
            numerator = 0
            for ngi in ngramsintersection:
                numerator += 1/(math.log( 1 + self.corpus['edges']['NGram'][ngi] ))
            denominator = 0
            for ngi in ngramsunion:
                denominator += 1/(math.log( 1 + self.corpus['edges']['NGram'][ngi] ))
            if denominator > 0:
                weight = numerator / denominator
            submatrix.set(document['id'], docid, value=weight, overwrite=True)
        return submatrix

    def diagonal( self, matrix_reducer ):
        for doc in self.documentngrams.iterkeys():
            matrix_reducer.set( doc, doc, value=self.corpus['edges']['Document'][doc])

    def generator(self):
        """
        uses Adjacency.walkDocuments() to return processed proximities
        """
        generator = self.walkDocuments()
        try:
            while 1:
                document = generator.next()
                yield self.proximity( generator.next() )
                # limit calculations to half a matrix
                del self.documentngrams[document['id']]
        except StopIteration, si:
            return
