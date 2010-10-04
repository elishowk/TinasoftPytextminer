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

def ngram_adj_task( config, storage, period, ngramgraphconfig, matrix_index ):
    try:
        ngramAdj = NgramAdjacency( config, storage, period, ngramgraphconfig, matrix_index )
        submatrix_gen = ngramAdj.walkDocuments()
        while 1:
            yield submatrix_gen.next()
    except Warning, warn:
        _logger.warning( str(warn) )
        return
    except StopIteration, si:
        _logger.debug("NgramAdjancency on period %s is finished"%period)

def document_adj_task( config, storage, period, documentgraphconfig, matrix_index):
    try:
        documentAdj = DocAdjacency( config, storage, period, documentgraphconfig, matrix_index )
        return documentAdj.walkDocuments()
    except Warning, warn:
        _logger.warning( str(warn) )
        return

class Matrix():
    """
    Matrix class to build adjacency matrix using numpy
    Manages external IDs with an auto-incremented internal index
    """
    def __init__(self, index, valuesize=int32):
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

    #def extract_matrix(self, minCooc=1):
        """
        yields all values of the upper part of the matrix
        associating ngrams with theirs tinasoft's id
        """
    #    countcooc = 0
    #    for tuple1, tuple2 in itertools.permutations( self.reverseindex.items(), 2 ):
    #        row = {}
    #        coocline = self.matrix[i,:]
    #        for cooc in coocline:
    #            if cooc >= minCooc:
    #                countcooc += 1
    #                ngj = self.id_index[j]
    #                row[ngj] = cooc
    #        if len(row.keys()) > 0:
    #            yield (ngi, row)
    #    _logger.debug("found %d non-zeros cooccurrences values"%countcooc)

class MatrixReducer(Matrix):
    """
    Generic matrix additioner
    """
    def __init__(self, *args, **kwargs):
        Matrix.__init__(self, *args, **kwargs)

    def add(self, submatrix):
        """"callback adding a submatrix into the self-contained matrix"""
        if submatrix is not None:
            for (external1, external2) in itertools.permutations( submatrix.reverseindex.keys(), 2):
                addvalue = submatrix.get( external1, external2 )
                if addvalue != 0:
                    self.set(external1, external2, value=addvalue, overwrite=False)


class Adjacency(object):
    """
    Base class
    A simple adjacency matrix processor
    """
    def __init__(self, config, storage, corpusid, opts, name, index):
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

    def proximity(self, document):
        raise Exception("proximity method not defined")
        return 0

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
                _logger.debug("setting up proximity to %s"%opts['proximity'])
                self.proximity = getattr(self, opts['proximity'])
                #del opts['proximity']
        except Exception, exc:
            _logger.error("impossible to load %s"%opts['proximity'])
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
                yield obj
                #self.proximity( obj )
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
    def __init__(self, config, storage, corpusid, opts, index):
        Adjacency.__init__(self, config, storage, corpusid, opts, 'NGramGraph', index)

    def cooccurrences( self, document ):
        submatrix = Matrix( document['edges']['NGram'].keys(), valuesize=float32 )
        valid_keys = set(document['edges']['NGram'].keys()) & set(self.corpus['edges']['NGram'].keys())
        map = dict.fromkeys(valid_keys, 1)
        # map is a unity slice of the matrix
        for (ngi, ngj) in itertools.combinations(map.keys(), 2):
            if ngi != ngj:
                submatrix.set( ngi, ngj, 1 )
                submatrix.set( ngj, ngi, 1 )
            else:
                submatrix.set( ngj, ngi, value=self.corpus['edges']['NGram'][ngi], overwrite=True )
        return submatrix

    def pseudoInclusion( self, document ):
        submatrix = self.cooccurrences( document )
        valid_keys = set(document['edges']['NGram'].keys()) & set(self.corpus['edges']['NGram'].keys())
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

    def walkDocuments(self):
        """
        walks through a list of documents into a corpus
        to process cooccurences
        """
        generator = super(NgramAdjacency, self).walkDocuments()
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
    def __init__(self, config, storage, corpusid, opts, index ):
        Adjacency.__init__(self, config, storage, corpusid, opts, 'DocumentGraph', index)

    def _loadDocument( self, docid ):
        if docid not in self.cache['Document']:
            self.cache['Document'][docid] = self.storage.loadDocument(docid)

    def sharedNgrams( self, document ):
        """
        intersection of doc's ngrams every other document
        then return length of the intersection with doc2 ngrams
        """
        submatrix = Matrix( self.corpus['edges']['Document'].keys(), valuesize=int32 )
        ngrams = set(document['edges']['NGram'].keys()) & set(self.corpus['edges']['NGram'].keys())
        documentobj = self.storage.loadDocument(docid)

        for docid in self.corpus['edges']['Document'].iterkeys():
            if docid != document['id']:
                documentobj = self.storage.loadDocument(docid)
                if documentobj is None: continue

                prox = len( ngrams & documentobj['edges']['NGram'].keys() )
                submatrix.set( document['id'], docid, value=prox )
                submatrix.set( docid, document['id'], value=prox )
            else:
                submatrix.set( docid, docid, value=self.corpus['edges']['Document'][docid], overwrite=True )
        return submatrix


    def logJaccard( self, document ):
        """
        Jaccard-like distance
        """
        submatrix = Matrix( self.corpus['edges']['Document'].keys(), valuesize=float32 )
        doc1ngrams = set(document['edges']['NGram'].keys())
        #self.documents[document['id']] = ngrams
        for docid in self.corpus['edges']['Document'].iterkeys():
            if docid == document['id']:
                submatrix.set( docid, docid, value=self.corpus['edges']['Document'][docid], overwrite=True )
            else:
                documentobj = self.storage.loadDocument(docid)
                if documentobj is None: continue

                doc2ngrams = set(documentobj['edges']['NGram'].keys())

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
                submatrix.set(docid, document['id'], weight)
        return submatrix

    def walkDocuments(self):
        """
        walks through a list of documents into a corpus
        to process cooccurences
        """
        generator = super(DocAdjacency, self).walkDocuments()
        try:
            while 1:
                document = generator.next()
                yield self.proximity( generator.next() )
        except StopIteration, si:
            return
