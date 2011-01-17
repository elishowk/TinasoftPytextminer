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

import itertools
from numpy import *
import math

from tinasoft.pytextminer import PyTextMiner, corpus

import logging
_logger = logging.getLogger('TinaAppLogger')

def process_ngram_subgraph(
        config,
        dataset,
        periods,
        ngram_index,
        doc_index,
        ngram_matrix_reducer,
        ngramgraphconfig,
        graphwriter,
        storage,
        ngram_graph_class
    ):
    """
    Generates ngram graph matrices from indexed dataset
    """
    for process_period in periods:
        ngram_args = ( config, storage, process_period, ngramgraphconfig, ngram_index, doc_index )
        adj = ngram_graph_class( *ngram_args )
        adj.diagonal(ngram_matrix_reducer)
        try:
            submatrix_gen = adj.generator()
            doccount = 0
            while 1:
                ngram_matrix_reducer.add( submatrix_gen.next() )
                doccount += 1
                yield doccount
        except Warning, warn:
            _logger.warning( str(warn) )
            return
        except StopIteration, si:
            _logger.debug("NGram sub-graph processed (%s) for period %s"%(
                    ngram_matrix_reducer.__class__.__name__,
                    process_period.id
                )
            )
    load_subgraph_gen = graphwriter.load_subgraph( 'NGram', ngram_matrix_reducer, subgraphconfig=ngramgraphconfig)
    try:
        while 1:
            # yields the updated node count
            yield load_subgraph_gen.next()
    except StopIteration, si:
        # finally yields the matrix reducer
        yield ngram_matrix_reducer
        return

def process_document_subgraph(
        config,
        dataset,
        periods,
        ngram_index,
        doc_index,
        doc_matrix_reducer,
        docgraphconfig,
        graphwriter,
        storage,
        doc_graph_class
    ):
    """
    Generates document graph from an indexed dataset
    Merging multiple periods in one meta object
    """
    metacorpus = corpus.Corpus("meta")
    for process_period in periods:
        metacorpus.updateEdges(process_period.edges)
        #metacorpus = PyTextMiner.updateEdges(process_period.edges, metacorpus)

    doc_args = ( config, storage, metacorpus, docgraphconfig, ngram_index, doc_index )
    adj = doc_graph_class( *doc_args )
    adj.diagonal(doc_matrix_reducer)
    try:
        submatrix_gen = adj.generator()
        doccount = 0
        while 1:
            doc_matrix_reducer.add( submatrix_gen.next() )
            doccount += 1
            yield doccount
    except Warning, warn:
        _logger.warning( str(warn) )
        return
    except StopIteration, si:
        _logger.debug("Document sub-graph processed (%s) for period %s"%(
                doc_matrix_reducer.__class__.__name__,
                metacorpus.id
            )
        )

    load_subgraph_gen = graphwriter.load_subgraph( 'Document', doc_matrix_reducer, subgraphconfig = docgraphconfig)
    try:
        while 1:
            # yields the updated node count
            yield load_subgraph_gen.next()
    except StopIteration, si:
        # finally yields the matrix reducer
        yield doc_matrix_reducer
        return

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
    Must use at least MatrixReducerFilter to get filtered graphs
    """
    def __init__(self, index, valuesize=float32):
        Matrix.__init__(self, index, valuesize=valuesize)

    def add(self, submatrix):
        """"callback adding a submatrix into the self-contained matrix"""
        if submatrix is not None:
            for (external1, external2) in itertools.permutations( submatrix.reverseindex.keys(), 2):
                self.set(external1, external2, value=submatrix.get( external1, external2 ), overwrite=False)

    def extract_semiupper_matrix(self, config, **kwargs):
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
                prox = float(self.array[i,i+j])
                if prox <= 0: continue
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

    def extract_matrix(self, config, **kwargs):
        """
        yields all rows from the matrix in a dictionary form
        filtering only nodes at this step (edges in a second step)
        yielded IDs are real tinasoft storage IDs
        """
        minnode = float(config['nodethreshold'][0])
        maxnode = float(config['nodethreshold'][1])
        # reverses the matrix reverse index
        id_index = {}
        for key, idx in self.reverseindex.iteritems():
            id_index[idx] = key
        # iterate over all rows and columns of the numpy matrix
        for i in range(self.array.shape[0]):
            # node filter
            nodeiweight = float(self.array[i,i])
            if nodeiweight < minnode or nodeiweight > maxnode:
                del id_index[i]
                continue
            row = {}
            nodei = id_index[i]
            for j in range(self.array.shape[0]):
                if i == j : continue
                nodejweight = float(self.array[j,j])
                # node filter
                if nodejweight < minnode or nodejweight > maxnode:
                    del id_index[j]
                    continue
                # converts any numpy.float to python float
                prox = float(self.array[i,j])
                #if prox <= 0:
                #    continue
                nodej = id_index[j]
                row[nodej] = prox
            #if len(row.keys()) > 0:
            yield (nodei, row)
        _logger.debug("MatrixReducer extracted %d valid nodes"%len(id_index.keys()))

    def export(self, path, index):
        """
        Utility for raw exports of the matrix
        """
        fh = open(path, 'w+')
        fh.write(",".join(index))
        savetxt( fh, self.array, "%.2f", ",")


class MatrixReducerFilter(MatrixReducer):
    """
    Simple matrix reducer : use it if there's no second level proximity calculation
    Only filtering edges values from MatrixReducer
    """
    def extract_matrix( self, config, **kwargs ):
        matrix = super(MatrixReducerFilter, self).extract_matrix( config )
        minedges = float(config['edgethreshold'][0])
        maxedges = float(config['edgethreshold'][1])
        #if 'hapax' in config:
        #    minedges = float(config['hapax'])
        try:
            count = 0
            while 1:
                nodei, row = matrix.next()
                for nodej in row.keys():
                    prox = row[nodej]
                    if maxedges is None and prox < minedges:
                        del row[nodej]
                    elif prox > maxedges or prox < minedges:
                        del row[nodej]
                    # otherwise, DOES NOTHING !
                count += len(row.keys())
                yield (nodei, row)
        except StopIteration, si:
            _logger.debug("MatrixReducerFilter generated %d valid edges"%count)


class Cooccurrences(MatrixReducerFilter): pass


class PseudoInclusion(MatrixReducer):
    """
    Matrix Reducer used to store cooccurrence matrix,
    then extract pseudo-inclusion on the fly
    """
    def extract_matrix( self, config, **kwargs ):
        alpha = config['alpha']
        minedges = float(config['edgethreshold'][0])
        maxedges = float(config['edgethreshold'][1])
        matrix = super(PseudoInclusion, self).extract_matrix( config )
        try:
            count = 0
            while 1:
                nodei, row = matrix.next()
                occi = self.get(nodei, nodei)
                for nodej in row.keys():
                    cooc = row[nodej]
                    occj = self.get(nodej, nodej)
                    # calculates the pseudo-inclusion prox
                    value = (( cooc / occi )**alpha) * (( cooc / occj )**(1/alpha))
                    if maxedges is None and value < minedges:
                        del row[nodej]
                    elif value > maxedges or value < minedges:
                        del row[nodej]
                    else:
                        row[nodej] = value
                count += len(row.keys())
                yield nodei, row
        except StopIteration:
            _logger.debug("PseudoInclusionMatrix generated %d valid edges"%count)


class EquivalenceIndex(MatrixReducer):
    """
    Implements Equivalence Index distance between two NGram nodes
    based on the mutual information of two NGrams
    """
    def extract_matrix( self, config, **kwargs ):
        """
        extract_matrix with an equivalence index proximity calculator
        """
        nb_documents = config['nb_documents']
        minedges = float(config['edgethreshold'][0])
        maxedges = float(config['edgethreshold'][1])
        matrix = super(EquivalenceIndex, self).extract_matrix( config )
        try:
            count = 0
            while 1:
                nodei, row = matrix.next()
                occi = self.get(nodei, nodei)
                for nodej in row.keys():
                    cooc = row[nodej]
                    occj = self.get(nodej, nodej)
                    # calculates the e-index
                    brut = float(cooc * nb_documents) / float(occi * occj)
                    if brut <= 0: continue
                    value = math.log( brut )
                    if maxedges is None and value < minedges:
                        del row[nodej]
                    elif value > maxedges or value < minedges:
                        del row[nodej]
                    else:
                        row[nodej] = value
                count += len(row.keys())
                yield nodei, row
        except StopIteration:
            _logger.debug("EquivalenceIndexMatrix generated %d valid edges"%count)


class SubGraph(object):
    """
    Base class
    A simple subgraph proximity matrix processor
    """
    def __init__( self, config, storage, corpus, opts, name, ngram_index, doc_index ):
        """
        Attach storage, a corpus objects and init the Matrix
        """
        self.storage = storage
        self.corpusid = corpus['id']
        self.corpus = corpus
        self.name = name
        self._loadDefaults(config)
        self._loadOptions(opts)
        self.ngram_index = ngram_index
        self.doc_index = doc_index

    def _loadDefaults(self, config):
        """
        loads default from config
        """
        for attr, value in config['datamining'][self.name].iteritems():
            setattr(self,attr,value)

    def _loadOptions(self, opts):
        """
        Overwrites config with eventual user parameters
        """
        for attr, value in opts.iteritems():
            setattr(self,attr,value)

    def _loadProximity(self):
        _logger.debug("%s setting getsubgraph() to %s"%(self.name,self.proximity))
        self.getsubgraph = getattr(self, self.proximity)

    def getsubgraph(self, document):
        raise Exception("proximity method not defined in %s"%self.name)
        return 0

    def walkDocuments(self):
        """
        walks through a list of documents within a corpus
        """
        totaldocs = len(self.doc_index)
        doccount = 0
        for doc_id in self.doc_index:
            obj = self.storage.loadDocument( doc_id )
            if obj is not None:
                yield obj
                doccount += 1
                self.notify( doccount, totaldocs )
            else:
                _logger.warning("%s cannot found document %s in database"%(self.name,doc_id))
        return

    def notify(self, doccount, totaldocs):
        if doccount % 50 == 0:
            _logger.debug(
                '%s processed %d of %d documents in period %s'\
                    %(self.name, doccount, totaldocs, self.corpusid)
            )



class NgramGraph(SubGraph):
    """
    A NGram subgraph edges processor
    """
    def __init__(self, config, storage, corpus, opts, ngram_index, doc_index ):
        SubGraph.__init__(self, config, storage, corpus, opts, 'NGramGraph', ngram_index, doc_index )

    def getsubgraph( self, ngid ):
        """
        gets preprocessed proximities from storage
        """
        submatrix = Matrix( list(self.ngram_index), valuesize=float32 )
        ngirow = self.storage.loadGraphPreprocess(self.corpusid+"::"+ngid, "NGram")
        if ngirow is None: return submatrix
        for ngj, stored_prox in ngirow.iteritems():
            if ngj not in self.ngram_index: continue
            submatrix.set( ngid, ngj, stored_prox )
        return submatrix

    def diagonal( self, matrix_reducer ):
        for ng, weight in self.corpus['edges']['NGram'].iteritems():
            matrix_reducer.set( ng, ng, value=weight)

    def generator(self):
        """
        walks through a list of documents into a corpus
        to process proximities
        """
        for ngid in self.ngram_index:
            yield self.getsubgraph( ngid )


class NgramGraphPreprocess(NgramGraph):
    """
    A NGram subgraph edges PRE processor
    """
    def getsubgraph( self, document ):
        """
        simple document cooccurrences matrix calculator
        """
        #valid_keys = set(document['edges']['NGram'].keys()) & self.ngram_index
        submatrix = Matrix( document['edges']['NGram'].keys(), valuesize=float32 )
        # only processes a half of the symmetric matrix
        for (ngi, ngj) in itertools.combinations(document['edges']['NGram'].keys(), 2):
            submatrix.set( ngi, ngj, 1 )
            submatrix.set( ngj, ngi, 1 )
        return submatrix

    def generator(self):
        """
        walks through a list of documents into a corpus
        to process proximities
        """
        docgenerator = self.walkDocuments()
        try:
            while 1:
                yield self.getsubgraph( docgenerator.next() )
        except StopIteration, si:
            return

class DocGraph(SubGraph):
    """
    A Document SubGraph edges processor
    'proximity' parameter that maps one of its method
    """
    def __init__( self, config, storage, corpus, opts, ngram_index, doc_index ):
        SubGraph.__init__(self, config, storage, corpus, opts, 'DocumentGraph', ngram_index, doc_index )
        # docgraph needs a custom proximity function
        self._loadProximity()
        # document's ngrams lists caching
        self.documentngrams = {}
        for doc in self.doc_index:
            documentobj = self.storage.loadDocument(doc)
            self.documentngrams[doc] = set(documentobj['edges']['NGram'].keys())

    def sharedNGrams( self, document ):
        """
        number of ngrams shared by 2 documents
        """
        submatrix = Matrix( self.doc_index, valuesize=float32 )
        doc1ngrams = self.documentngrams[document['id']]
        for docid in self.documentngrams.keys():
            if docid != document['id']:
                doc2ngrams = self.documentngrams[docid]
                prox = len( doc1ngrams & doc2ngrams )
                submatrix.set( document['id'], docid, value=prox, overwrite=True )
                submatrix.set( docid, document['id'], value=prox, overwrite=True )

        return submatrix

    def logJaccard( self, document ):
        """
        Jaccard-like similarity distance
        Invalid if summing values avor many periods
        """
        submatrix = Matrix( self.doc_index, valuesize=float32 )
        doc1ngrams = self.documentngrams[document['id']]

        for docid in self.documentngrams.keys():
            if docid != document['id']:
                doc2ngrams = self.documentngrams[docid]
                ngramsintersection = set(self.corpus['edges']['NGram'])
                ngramsunion = set(self.corpus['edges']['NGram'])
                ngramsintersection &= doc1ngrams & doc2ngrams
                ngramsunion &= doc1ngrams | doc2ngrams

                numerator = 0
                for ngi in ngramsintersection:
                    numerator += 1/(math.log( 1 + self.corpus['edges']['NGram'][ngi] ))
                denominator = 0
                for ngi in ngramsunion:
                    denominator += 1/(math.log( 1 + self.corpus['edges']['NGram'][ngi] ))
                weight = 0
                if denominator > 0:
                    weight = numerator / denominator
                submatrix.set(document['id'], docid, value=weight, overwrite=True)
                submatrix.set(docid, document['id'], value=weight, overwrite=True)
        return submatrix

    def diagonal( self, matrix_reducer ):
        for doc, weight in self.corpus['edges']['Document'].iteritems():
            matrix_reducer.set( doc, doc, value=weight)

    def generator(self):
        """
        uses SubGraph.walkDocuments() to yield processed proximities
        """
        generator = self.walkDocuments()
        try:
            while 1:
                document = generator.next()
                yield self.getsubgraph( document )
                #### CAUTION : this limits processing to an half of the matrix
                del self.documentngrams[document['id']]
        except StopIteration, si:
            return
